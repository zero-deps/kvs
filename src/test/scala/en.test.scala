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
import scala.collection.immutable.ArraySeq

object EnHandlerTest {
  val fid = "fid" + java.util.UUID.randomUUID.toString
}

class EnHandlerTest extends TestKit(ActorSystem("Test", ConfigFactory.parseString(conf.tmpl(port=4012))))
  with AnyFreeSpecLike with Matchers with BeforeAndAfterAll {

  import EnHandlerTest.fid

  def entry(n: Int): AddAuto = AddAuto(fid, data=ArraySeq.from(s"val=${n}".getBytes))
  def e_copy(en: AddAuto, id: String): En = En(id=id, prev=None, data=en.data)
  def e_copy(en: AddAuto, id: String, prev: String): En = En(id=id, prev=prev.just, data=en.data)
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
      kvs.all(fid) shouldBe (Right(LazyList.empty))
    }

    "should save e1" in {
      val saved = kvs.add(e1).fold(l => l match { 
        case Throwed(x) => throw x
        case _ => ???
      }, identity)
      kvs.fd.get(Fd(fid)).map(_.get.count) match {
        case Right(x) => x shouldBe 1
        case Left(Throwed(t)) => t.printStackTrace
        case Left(x) => fail(x.toString)
      }
      (saved.id, saved.data) shouldBe ("1", e1.data)
    }

    "should save e2" in {
      val saved = kvs.add(e2).fold(l => { println(l); ??? }, identity)
      kvs.fd.get(Fd(fid)).fold(l => { println(l); ??? }, identity).get.count shouldBe 2
      (saved.id, saved.data) shouldBe ("2", e2.data)
    }

    "should get e1 and e2 from feed" in {
      kvs.fd.get(Fd(fid)).fold(l => { println(l); ??? }, identity).get.count shouldBe 2

      val stream = kvs.all(fid)
      stream.map(_.toList) shouldBe List(e_copy(e2, id="2",prev="1").right, e_copy(e1, id="1").right).right
    }

    "should save entry(3)" in {
      val saved = kvs.add(e3).fold(l => { println(l); ??? }, identity)
      (saved.id, saved.data) shouldBe ("3", e3.data)
    }

    "should not save entry(2) again" in {
      kvs.add(Add(fid=e2.fid, id="2", data=e2.data)).fold(identity, identity) shouldBe EntryExists(s"${fid}.2")
    }

    "should get 3 values from feed" in {
      kvs.fd.get(Fd(fid)).fold(l => { println(l); ??? }, identity).get.count shouldBe 3

      val stream = kvs.all(fid)
      stream.map(_.toList) shouldBe List(e_copy(e3, id="3",prev="2").right, e_copy(e2, id="2",prev="1").right, e_copy(e1, id="1").right).right
    }

    "should remove unexisting entry from feed without error" in {
      kvs.remove(fid,"5").fold(identity, identity) shouldBe None
    }

    "should remove entry(2) from feed without prev/next/data" in {
      val deleted = kvs.remove(e2.fid,"2").fold(l => { println(l); ??? }, _.getOrElse(???))

      (deleted.id, deleted.data) shouldBe ("2", e2.data)
    }

    "should get 2 values from feed" in {
      kvs.fd.get(Fd(fid)).fold(l => { println(l); ??? }, identity).get.count shouldBe 2

      val stream = kvs.all(fid)
      stream.map(_.toList) shouldBe List(e_copy(e3, id="3",prev="1").right, e_copy(e1, id="1").right).right
    }

    "should remove entry(1) from feed" in {
      val deleted = kvs.remove(fid,"1").fold(l => { println(l); ??? }, _.getOrElse(???))

      (deleted.id, deleted.data) shouldBe ("1", e1.data)
    }

    "should get 1 values from feed" in {
      kvs.fd.get(Fd(fid)).fold(l => { println(l); ??? }, identity).get.count shouldBe 1

      val stream = kvs.all(fid)
      stream.map(_.toList) shouldBe List(e_copy(e3, id="3").right).right
    }

    "should remove entry(3) from feed" in {
      val deleted = kvs.remove(fid,"3").fold(l => { println(l); ??? }, _.getOrElse(???))

      (deleted.id, deleted.data) shouldBe ("3", e3.data)
    }

    "should be empty" in {
      kvs.fd.get(Fd(fid)).getOrElse(???).get.count shouldBe 0
      kvs.all(fid).getOrElse(???) shouldBe empty
    }

    "should not create stack overflow" in {
      val limit = 100

      LazyList.from(1,1).takeWhile( _.<=(limit)).foreach{ n =>
        val toadd = entry(n)
        val added = kvs.add(toadd).fold(l => { println(l); ??? }, identity)
        (added.id, added.data) shouldBe ((n+3).toString, toadd.data)
      }

      LazyList.from(1,1).takeWhile( _.<=(limit)).foreach{ n =>

        val toremove = e_copy(entry(n), id=(n+3).toString)
        val removed = kvs.remove(entry(n).fid, toremove.id).fold(l => { println(l); ??? }, _.getOrElse(???))

        (removed.id, removed.data) shouldBe ((n+3).toString, toremove.data)

        kvs.fd.get(Fd(fid)).fold(l => { println(l); ??? }, identity).get.count shouldBe (limit - n)
      }
    }

    "feed should be empty at the end test" in {
      kvs.fd.get(Fd(fid)).getOrElse(???).get.count shouldBe 0
      kvs.all(fid).getOrElse(???) shouldBe empty
      kvs.fd.delete(Fd(fid)) shouldBe Right(())
      kvs.fd.delete(Fd(fid)) shouldBe Right(()) // idempotent
      kvs.all(fid) shouldBe (Right(LazyList.empty))
    }
  }
}
