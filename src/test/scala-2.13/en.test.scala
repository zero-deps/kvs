package zd.kvs
package en

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

object EnHandlerTest {
  final case class En(fid: String, id: String = empty, prev: String = empty, data: String) extends zd.kvs.en.En

  implicit val h: EnHandler[En] = new EnHandler[En] {
    val fh = feedHandler
    def pickle(e: En): Res[Array[Byte]] = Right(s"${e.fid}^${e.id}^${e.prev}^${e.data}".getBytes)
    def unpickle(a: Array[Byte]): Res[En] = new String(a).split('^') match {
      case Array(fid, id, prev, data) => Right(En(fid, id, prev, data))
      case _ => Left(UnpickleFail(new Exception("bad data")))
    }
    override protected def update(en: En, id: String, prev: String): En = en.copy(id = id, prev = prev)
    override protected def update(en: En, prev: String): En = en.copy(prev = prev)
  }

  implicit object feedHandler extends FdHandler {
    def pickle(e: Fd): Res[Array[Byte]] = Right(s"${e.id}^${e.top}^${e.count}".getBytes)
    def unpickle(a: Array[Byte]): Res[Fd] = new String(a).split('^') match {
      case Array(id, top, count) => Right(Fd(id, top, count.toInt))
      case _ => Left(UnpickleFail(new Exception("bad data")))
    }
  }
}

class EnHandlerTest extends AnyFreeSpecLike with Matchers with BeforeAndAfterAll {
  import EnHandlerTest._

  val mod = 50
  val fid = "fid" + java.util.UUID.randomUUID.toString
  def entry(n: Int): En = En(fid, id=n.toString, data=s"value=${n}")

  val e1 = entry(1)
  val e2 = entry(2)
  val e3 = entry(3)
  val e5 = entry(5)

  val kvs = Kvs.mem()

  "Feed should" - {
    "be empty at creation" in {
      kvs.all[En](fid) shouldBe (Right(LazyList.empty))
    }

    "should save e1" in {
      val saved = kvs.add(e1).getOrElse(???)
      kvs.fd.get(Fd(fid)).map(_.get.count) match {
        case Right(x) => x shouldBe 1
        case Left(RngThrow(t)) => t.printStackTrace
        case Left(x) => fail(x.toString)
      }
      (saved.fid, saved.id, saved.data) shouldBe ((e1.fid, "1", e1.data))
    }

    "should save e2" in {
      val saved = kvs.add(e2).getOrElse(???)
      kvs.fd.get(Fd(fid)).getOrElse(???).get.count shouldBe 2
      (saved.fid, saved.id, saved.data) shouldBe ((e2.fid, "2", e2.data))
    }

    "should get e1 and e2 from feed" in {
      kvs.fd.get(Fd(fid)).getOrElse(???).get.count shouldBe 2

      val stream = kvs.all[En](fid)
      stream.map(_.toList) shouldBe Right(List(Right(e2.copy(prev="1")), Right(e1)))
    }

    "should save entry(3)" in {
      val saved = kvs.add(e3).getOrElse(???)
      (saved.fid, saved.id, saved.data) shouldBe ((e3.fid, "3", e3.data))
    }

    "should not save entry(2) again" in {
      kvs.add(e2).left.getOrElse(???) shouldBe EntryExists(s"${fid}.2")
    }

    "should get 3 values from feed" in {
      kvs.fd.get(Fd(fid)).getOrElse(???).get.count shouldBe 3

      val stream = kvs.all[En](fid)
      stream.map(_.toList) shouldBe Right(List(Right(e3.copy(prev="2")), Right(e2.copy(prev="1")), Right(e1)))
    }

    "should not remove unexisting entry from feed" in {
      kvs.remove(fid,"5").left.getOrElse(???) shouldBe NotFound(s"${fid}.5")
    }

    "should remove entry(2) from feed without prev/next/data" in {
      val deleted = kvs.remove(e2.fid,"2").getOrElse(???)

      (deleted.fid, deleted.id, deleted.data) shouldBe ((e2.fid, "2", e2.data))
    }

    "should get 2 values from feed" in {
      kvs.fd.get(Fd(fid)).getOrElse(???).get.count shouldBe 2

      val stream = kvs.all[En](fid)
      stream.map(_.toList) shouldBe Right(List(Right(e3.copy(prev="1")), Right(e1)))
    }

    "should remove entry(1) from feed" in {
      val deleted = kvs.remove(fid,"1").getOrElse(???)

      (deleted.fid, deleted.id, deleted.data) shouldBe ((e1.fid, "1", e1.data))
    }

    "should get 1 values from feed" in {
      kvs.fd.get(Fd(fid)).getOrElse(???).get.count shouldBe 1

      val stream = kvs.all[En](fid)
      stream.map(_.toList) shouldBe Right(List(Right(e3)))
    }

    "should remove entry(3) from feed" in {
      val deleted = kvs.remove(fid,"3").getOrElse(???)

      (deleted.fid, deleted.id, deleted.data) shouldBe ((e3.fid, "3", e3.data))
    }

    "should be empty" in {
      kvs.fd.get(Fd(fid)).getOrElse(???).get.count shouldBe 0
      kvs.all[En](fid).getOrElse(???) shouldBe empty
    }

    "should remove after first entry" in {
      kvs.add(e1); kvs.add(e2); kvs.add(e3)
      kvs.fd.get(Fd(fid)).getOrElse(???).get.count shouldBe 3
      kvs.removeAfter(kvs.head(fid).getOrElse(???).getOrElse(???))
      kvs.fd.get(Fd(fid)).getOrElse(???).get.count shouldBe 1
      kvs.remove(fid, "3")
      kvs.fd.get(Fd(fid)).getOrElse(???).get.count shouldBe 0
    }

    "should be empty after `clearFeed`" in {
      kvs.add(e1); kvs.add(e2); kvs.add(e3)
      kvs.fd.get(Fd(fid)).getOrElse(???).get.count shouldBe 3
      kvs.clearFeed(fid)
      kvs.fd.get(Fd(fid)).getOrElse(???) shouldBe None
      kvs.all[En](fid).getOrElse(???) shouldBe empty

      val added3 = kvs.add(e3).getOrElse(???)
      (added3.fid, added3.id, added3.data) shouldBe ((e3.fid, e3.id, e3.data))
      val added1 = kvs.add(e1).getOrElse(???) 
      (added1.fid, added1.id, added1.data) shouldBe ((e1.fid, e1.id, e1.data))
      kvs.clearFeed(fid)
    }

    "should not create stack overflow" in {
      val limit = 100
      LazyList.from(1,1).takeWhile( _.<=(limit)).foreach{ n =>
        val toadd = entry(n)
        val added = kvs.add(toadd).getOrElse(???)
        (added.fid, added.id, added.data) shouldBe ((toadd.fid, n.toString, toadd.data))
      }
      LazyList.from(1,1).takeWhile( _.<=(limit)).foreach{ n =>
        val toremove = entry(n)
        val removed = kvs.remove(toremove.fid, toremove.id).getOrElse(???)
        (removed.fid, removed.id, removed.data) shouldBe ((toremove.fid, n.toString, toremove.data))
        kvs.fd.get(Fd(fid)).getOrElse(???).get.count shouldBe (limit - n)
      }
    }

    "feed should be empty at the end test" in {
      kvs.el.delete[String](s"IdCounter.$fid")
      kvs.fd.get(Fd(fid)).getOrElse(???).get.count shouldBe 0
      kvs.all[En](fid).getOrElse(???) shouldBe empty
      kvs.fd.delete(Fd(fid))
      kvs.all[En](fid) shouldBe (Right(LazyList.empty))
    }
  }
}
