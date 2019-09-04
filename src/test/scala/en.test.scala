package zd.kvs
package en

import akka.actor.ActorSystem
import akka.testkit._
import com.typesafe.config.{ConfigFactory}
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try
import zd.gs.z._

object EnHandlerTest {
  val fid = "fid" + java.util.UUID.randomUUID.toString

  final case class En(fid: String, id_opt: Option[String]=None, prev_opt: Option[String]=None, data: String) extends zd.kvs.en.En

  implicit val h = new EnHandler[En] {
    val fh = feedHandler
    def pickle(e: En): Res[Array[Byte]] = s"${e.fid}^${e.id_opt.getOrElse("none")}^${e.prev_opt.getOrElse("none")}^${e.data}".getBytes.right
    def unpickle(a: Array[Byte]): Res[En] = new String(a).split('^') match {
      case Array(fid, id, prev, data) => En(fid, if (id == "none") None else id.just, if (prev == "none") None else prev.just, data).right
      case _ => UnpickleFail(new Exception("bad data")).left
    }
    override protected def update(en: En, id: Option[String], prev: Option[String]): En = en.copy(id_opt = id, prev_opt = prev)
    override protected def update(en: En, prev: Option[String]): En = en.copy(prev_opt = prev)
  }

  implicit object feedHandler extends FdHandler {
    def pickle(e: Fd): Res[Array[Byte]] = s"${e.id}^${e.top_opt.getOrElse("none")}^${e.count}".getBytes.right
    def unpickle(a: Array[Byte]): Res[Fd] = new String(a).split('^') match {
      case Array(id, top, count) => Fd(id, if (top == "none") None else top.just, count.toInt).right
      case _ => UnpickleFail(new Exception("bad data")).left
    }
  }
}

class EnHandlerTest extends TestKit(ActorSystem("Test", ConfigFactory.parseString(conf.tmpl(port=4012))))
  with AnyFreeSpecLike with Matchers with BeforeAndAfterAll {

  import EnHandlerTest._

  val mod = 50
  def entry(n: Int): En = En(fid, data=s"val=${n}")
  def e_copy(en: En, id: String): En = En(fid=en.fid, id_opt=id.just, prev_opt=None, data=en.data)
  def e_copy(en: En, id: String, prev: String): En = En(fid=en.fid, id_opt=id.just, prev_opt=prev.just, data=en.data)

  val e1 = entry(1)
  val e2 = entry(2)
  val e3 = entry(3)
  val e5 = entry(5)

  var kvs: Kvs = null
  override def beforeAll = {
    kvs = Kvs(system)
    Try(Await.result(kvs.onReady, FiniteDuration(1, MINUTES)))
  }
  override def afterAll = TestKit.shutdownActorSystem(system)

  "feed should" - {
    "be empty at creation" in {
      kvs.all[En](fid) shouldBe (Right(LazyList.empty))
    }

    "should save e1" in {
      val saved = kvs.add(e1).fold(l => l match { 
        case RngThrow(x) => throw x
        case _ => ???
      }, identity)
      kvs.fd.get(Fd(fid)).map(_.get.count) match {
        case Right(x) => x shouldBe 1
        case Left(RngThrow(t)) => t.printStackTrace
        case Left(x) => fail(x.toString)
      }
      (saved.fid, saved.id_opt.getOrElse(???), saved.data) shouldBe (e1.fid, "1", e1.data)
    }

    "should save e2" in {
      val saved = kvs.add(e2).fold(l => { println(l); ??? }, identity)
      kvs.fd.get(Fd(fid)).fold(l => { println(l); ??? }, identity).get.count shouldBe 2
      (saved.fid, saved.id_opt.getOrElse(???), saved.data) shouldBe (e2.fid, "2", e2.data)
    }

    "should get e1 and e2 from feed" in {
      kvs.fd.get(Fd(fid)).fold(l => { println(l); ??? }, identity).get.count shouldBe 2

      val stream = kvs.all[En](fid)
      stream.map(_.toList) shouldBe List(e_copy(e2, id="2",prev="1").right, e_copy(e1, id="1").right).right
    }

    "should save entry(3)" in {
      val saved = kvs.add(e3).fold(l => { println(l); ??? }, identity)
      (saved.fid, saved.id_opt.getOrElse(???), saved.data) shouldBe (e3.fid, "3", e3.data)
    }

    "should not save entry(2) again" in {
      kvs.add(e2.copy(id_opt="2".just)).fold(identity, identity) shouldBe EntryExists(s"${fid}.2")
    }

    "should get 3 values from feed" in {
      kvs.fd.get(Fd(fid)).fold(l => { println(l); ??? }, identity).get.count shouldBe 3

      val stream = kvs.all[En](fid)
      stream.map(_.toList) shouldBe List(e_copy(e3, id="3",prev="2").right, e_copy(e2, id="2",prev="1").right, e_copy(e1, id="1").right).right
    }

    "should remove unexisting entry from feed without error" in {
      kvs.remove_opt(fid,"5").fold(identity, identity) shouldBe None
    }

    "should remove entry(2) from feed without prev/next/data" in {
      val deleted = kvs.remove_opt(e2.fid,"2").fold(l => { println(l); ??? }, _.getOrElse(???))

      (deleted.fid, deleted.id_opt.getOrElse(???), deleted.data) shouldBe (e2.fid, "2", e2.data)
    }

    "should get 2 values from feed" in {
      kvs.fd.get(Fd(fid)).fold(l => { println(l); ??? }, identity).get.count shouldBe 2

      val stream = kvs.all[En](fid)
      stream.map(_.toList) shouldBe List(e_copy(e3, id="3",prev="1").right, e_copy(e1, id="1").right).right
    }

    "should remove entry(1) from feed" in {
      val deleted = kvs.remove_opt(fid,"1").fold(l => { println(l); ??? }, _.getOrElse(???))

      (deleted.fid, deleted.id_opt.getOrElse(???), deleted.data) shouldBe (e1.fid, "1", e1.data)
    }

    "should get 1 values from feed" in {
      kvs.fd.get(Fd(fid)).fold(l => { println(l); ??? }, identity).get.count shouldBe 1

      val stream = kvs.all[En](fid)
      stream.map(_.toList) shouldBe List(e_copy(e3, id="3").right).right
    }

    "should remove entry(3) from feed" in {
      val deleted = kvs.remove_opt(fid,"3").fold(l => { println(l); ??? }, _.getOrElse(???))

      (deleted.fid, deleted.id_opt.getOrElse(???), deleted.data) shouldBe (e3.fid, "3", e3.data)
    }

    "should be empty" in {
      kvs.fd.get(Fd(fid)).getOrElse(???).get.count shouldBe 0
      kvs.all[En](fid).getOrElse(???) shouldBe empty
    }

    "should not create stack overflow" in {
      val limit = 100

      LazyList.from(1,1).takeWhile( _.<=(limit)).foreach{ n =>
        val toadd = entry(n)
        val added = kvs.add(toadd).fold(l => { println(l); ??? }, identity)
        (added.fid, added.id_opt.getOrElse(???), added.data) shouldBe (toadd.fid, (n+3).toString, toadd.data)
      }

      LazyList.from(1,1).takeWhile( _.<=(limit)).foreach{ n =>

        val toremove = e_copy(entry(n), id=(n+3).toString)
        val removed = kvs.remove_opt(toremove.fid, toremove.id_opt.getOrElse(???)).fold(l => { println(l); ??? }, _.getOrElse(???))

        (removed.fid, removed.id_opt.getOrElse(???), removed.data) shouldBe (toremove.fid, (n+3).toString, toremove.data)

        kvs.fd.get(Fd(fid)).fold(l => { println(l); ??? }, identity).get.count shouldBe (limit - n)
      }
    }

    "feed should be empty at the end test" in {
      kvs.fd.get(Fd(fid)).getOrElse(???).get.count shouldBe 0
      kvs.all[En](fid).getOrElse(???) shouldBe empty
      kvs.fd.delete(Fd(fid)) shouldBe Right(())
      kvs.fd.delete(Fd(fid)) shouldBe Right(()) // idempotent
      kvs.all[En](fid) shouldBe (Right(LazyList.empty))
    }
  }
}
