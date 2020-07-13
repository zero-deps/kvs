package zd.kvs
package en

import akka.actor.ActorSystem
import akka.testkit._
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try
import zero.ext._, either._

object EnHandlerTest {
  final case class En(fid: String, id: String = empty, prev: String = empty, data: String) extends zd.kvs.en.En

  implicit val h = new EnHandler[En] {
    val fh = feedHandler
    def pickle(e: En): Res[Array[Byte]] = s"${e.fid}^${e.id}^${e.prev}^${e.data}".getBytes.right
    def unpickle(a: Array[Byte]): Res[En] = new String(a).split('^') match {
      case Array(fid, id, prev, data) => En(fid, id, prev, data).right
      case _ => UnpickleFail(new Exception("bad data")).left
    }
    override protected def update(en: En, id: String, prev: String): En = en.copy(id = id, prev = prev)
    override protected def update(en: En, prev: String): En = en.copy(prev = prev)
  }

  implicit object feedHandler extends FdHandler {
    def pickle(e: Fd): Res[Array[Byte]] = s"${e.id}^${e.top}^${e.count}".getBytes.right
    def unpickle(a: Array[Byte]): Res[Fd] = new String(a).split('^') match {
      case Array(id, top, count) => Fd(id, top, count.toInt).right
      case _ => UnpickleFail(new Exception("bad data")).left
    }
  }
}

class EnHandlerTest extends TestKit(ActorSystem("Test", ConfigFactory.parseString(conf.tmpl(port=4012))))
  with AnyFreeSpecLike with Matchers with BeforeAndAfterAll {

  import EnHandlerTest._

  val mod = 50
  val fid = "fid" + java.util.UUID.randomUUID.toString
  def entry(n: Int): En = En(fid, data=s"value=${n}")

  val e1 = entry(1)
  val e2 = entry(2)
  val e3 = entry(3)
  val e5 = entry(5)

  var kvs: Kvs = null
  override def beforeAll(): Unit = {
    kvs = Kvs(system)
    Try(Await.result(kvs.onReady, FiniteDuration(1, MINUTES)))
  }
  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

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
      (saved.fid, saved.id, saved.data) shouldBe(e1.fid, "1", e1.data)
    }

    "should save e2" in {
      val saved = kvs.add(e2).getOrElse(???)
      kvs.fd.get(Fd(fid)).getOrElse(???).get.count shouldBe 2
      (saved.fid, saved.id, saved.data) shouldBe(e2.fid, "2", e2.data)
    }

    "should get e1 and e2 from feed" in {
      kvs.fd.get(Fd(fid)).getOrElse(???).get.count shouldBe 2

      val stream = kvs.all[En](fid)
      stream.map(_.toList) shouldBe List(e2.copy(id="2",prev="1").right, e1.copy(id="1").right).right
    }

    "should save entry(3)" in {
      val saved = kvs.add(e3).getOrElse(???)
      (saved.fid, saved.id, saved.data) shouldBe(e3.fid, "3", e3.data)
    }

    "should not save entry(2) again" in {
      kvs.add(e2.copy(id="2")).left.getOrElse(???) shouldBe EntryExists(s"${fid}.2")
    }

    "should get 3 values from feed" in {
      kvs.fd.get(Fd(fid)).getOrElse(???).get.count shouldBe 3

      val stream = kvs.all[En](fid)
      stream.map(_.toList) shouldBe List(e3.copy(id="3",prev="2").right, e2.copy(id="2",prev="1").right, e1.copy(id="1").right).right
    }

    "should not remove unexisting entry from feed" in {
      kvs.remove(fid,"5").left.getOrElse(???) shouldBe NotFound(s"${fid}.5")
    }

    "should remove entry(2) from feed without prev/next/data" in {
      val deleted = kvs.remove(e2.fid,"2").getOrElse(???)

      (deleted.fid, deleted.id, deleted.data) shouldBe(e2.fid, "2", e2.data)
    }

    "should get 2 values from feed" in {
      kvs.fd.get(Fd(fid)).getOrElse(???).get.count shouldBe 2

      val stream = kvs.all[En](fid)
      stream.map(_.toList) shouldBe List(e3.copy(id="3",prev="1").right, e1.copy(id="1").right).right
    }

    "should remove entry(1) from feed" in {
      val deleted = kvs.remove(fid,"1").getOrElse(???)

      (deleted.fid, deleted.id, deleted.data) shouldBe(e1.fid, "1", e1.data)
    }

    "should get 1 values from feed" in {
      kvs.fd.get(Fd(fid)).getOrElse(???).get.count shouldBe 1

      val stream = kvs.all[En](fid)
      stream.map(_.toList) shouldBe List(e3.copy(id="3").right).right
    }

    "should remove entry(3) from feed" in {
      val deleted = kvs.remove(fid,"3").getOrElse(???)

      (deleted.fid, deleted.id, deleted.data) shouldBe(e3.fid, "3", e3.data)
    }

    "should be empty" in {
      kvs.fd.get(Fd(fid)).getOrElse(???).get.count shouldBe 0
      kvs.all[En](fid).getOrElse(???) shouldBe empty
    }

    "should not create stack overflow" in {
      val limit = 100

      LazyList.from(1,1).takeWhile( _.<=(limit)).foreach{ n =>
        val toadd = entry(n)
        val added = kvs.add(toadd).getOrElse(???)
        (added.fid, added.id, added.data) shouldBe (toadd.fid, (n+3).toString, toadd.data)
      }

      LazyList.from(1,1).takeWhile( _.<=(limit)).foreach{ n =>

        val toremove = entry(n).copy(id=(n+3).toString)
        val removed = kvs.remove(toremove.fid, toremove.id).getOrElse(???)

        (removed.fid, removed.id, removed.data) shouldBe (toremove.fid, (n+3).toString, toremove.data)

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
