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

  def data(n: Int): ArraySeq[Byte] = ArraySeq.unsafeWrapArray(s"val=${n}".getBytes)

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
      val saved = kvs.add(fid, "1", data(1)).fold(l => l match { 
        case Throwed(x) => throw x
        case _ => ???
      }, identity)
      kvs.fd.get(Fd(fid)).map(_.get.count) match {
        case Right(x) => x shouldBe 1
        case Left(Throwed(t)) => t.printStackTrace
        case Left(x) => fail(x.toString)
      }
      (saved.id, saved.data) shouldBe ("1", data(1))
    }

    "should save e2" in {
      val saved = kvs.add(fid, "2", data(2)).fold(l => { println(l); ??? }, identity)
      kvs.fd.get(Fd(fid)).fold(l => { println(l); ??? }, identity).get.count shouldBe 2
      (saved.id, saved.data) shouldBe ("2", data(2))
    }

    "should get e1 and e2 from feed" in {
      kvs.fd.get(Fd(fid)).fold(l => { println(l); ??? }, identity).get.count shouldBe 2

      val stream = kvs.all(fid)
      stream.map(_.toList) shouldBe List(En("2", "1".just, data(2)).right, En("1", None, data(1)).right).right
    }

    "should save entry(3)" in {
      val saved = kvs.add(fid, "3", data(3)).fold(l => { println(l); ??? }, identity)
      (saved.id, saved.data) shouldBe ("3", data(3))
    }

    "should not save entry(2) again" in {
      kvs.add(fid, "2", data(2)).fold(identity, identity) shouldBe EntryExists(s"${fid}.2")
    }

    "should get 3 values from feed" in {
      kvs.fd.get(Fd(fid)).fold(l => { println(l); ??? }, identity).get.count shouldBe 3

      val stream = kvs.all(fid)
      stream.map(_.toList) shouldBe List(En("3", "2".just, data(3)).right, En("2", "1".just, data(2)).right, En("1", Nothing, data(1)).right).right
    }

    "should remove unexisting entry from feed without error" in {
      kvs.remove(fid, "5") shouldBe Right(None)
    }

    "should remove entry(2) from feed without prev/next/data" in {
      val deleted = kvs.remove(fid, "2")
      deleted.map(_.map(_.id)) shouldBe "2".just.right
      deleted.map(_.map(_.data)) shouldBe data(2).just.right
    }

    "should get 2 values from feed" in {
      kvs.fd.get(Fd(fid)).map(_.map(_.count)) shouldBe 2.just.right

      val stream = kvs.all(fid)
      stream.map(_.toList) shouldBe List(En("3", "1".just, data(3)).right, En("1", None, data(1)).right).right
    }

    "should remove entry(1) from feed" in {
      val deleted = kvs.remove(fid, "1")
      deleted.map(_.map(_.id)) shouldBe "1".just.right
      deleted.map(_.map(_.data)) shouldBe data(1).just.right
    }

    "should get 1 values from feed" in {
      kvs.fd.get(Fd(fid)).map(_.map(_.count)) shouldBe 1.just.right

      val stream = kvs.all(fid)
      stream.map(_.toList) shouldBe List(En("3", Nothing, data(3)).right).right
    }

    "should remove entry(3) from feed" in {
      val deleted = kvs.remove(fid, "3")
      deleted.map(_.map(_.id)) shouldBe "3".just.right
      deleted.map(_.map(_.data)) shouldBe data(3).just.right
    }

    "should be empty" in {
      kvs.fd.get(Fd(fid)).map(_.map(_.count)) shouldBe 0.just.right
      kvs.all(fid) shouldBe LazyList.empty.right
    }

    "should not create stack overflow" in {
      val limit = 100
      LazyList.from(start=1, step=1).takeWhile(_ <= limit).foreach{ n =>
        val added = kvs.add(fid, data(n))
        added.map(_.id) shouldBe (n+3).toString
        added.map(_.data) shouldBe data(n)
      }
      LazyList.from(start=1, step=1).takeWhile(_ <= limit).foreach{ n =>
        val removed = kvs.remove(fid, n.toString)
        removed.map(_.map(_.id)) shouldBe (n+3).toString
        removed.map(_.map(_.data)) shouldBe data(3)
        kvs.fd.get(Fd(fid)).map(_.map(_.count)) shouldBe (limit - n).just.right
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
