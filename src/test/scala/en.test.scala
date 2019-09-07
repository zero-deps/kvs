package zd.kvs
package en

import akka.actor.ActorSystem
import akka.testkit._
import com.typesafe.config.{ConfigFactory}
import org.scalatest._
import org.scalatest.freespec.AnyFreeSpecLike
import scala.collection.immutable.ArraySeq
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try
import zd.gs.z._

class FeedSpec extends TestKit(ActorSystem("Test", ConfigFactory.parseString(conf.tmpl(port=4012))))
  with AnyFreeSpecLike with Matchers with BeforeAndAfterAll {

  def data(n: Int): ArraySeq[Byte] = ArraySeq.unsafeWrapArray(s"val=${n}".getBytes)

  var kvs: Kvs = null
  override def beforeAll = {
    kvs = Kvs(system)
    Try(Await.result(kvs.onReady, FiniteDuration(1, MINUTES)))
  }
  override def afterAll = TestKit.shutdownActorSystem(system)

  "feed" - {
    "no 1" - {
      val fid = "fid1" + java.util.UUID.randomUUID.toString
      
      "should be empty at creation" in {
        kvs.all(fid) shouldBe (Right(LazyList.empty))
      }

      "should save e1" in {
        val saved = kvs.add(fid, "1", data(1))
        saved.map(_.id) shouldBe "1".right
        saved.map(_.data) shouldBe data(1).right
        kvs.fd.length(fid) shouldBe 1L.right
      }

      "should save e2" in {
        val saved = kvs.add(fid, "2", data(2))
        saved.map(_.id) shouldBe "2".right
        saved.map(_.data) shouldBe data(2).right
        kvs.fd.length(fid) shouldBe 2L.right
      }

      "should get e1 and e2 from feed" in {
        kvs.fd.length(fid) shouldBe 2L.right 
        kvs.all(fid).map(_.toList) shouldBe List(En("2", "1".just, data(2)).right, En("1", Nothing, data(1)).right).right
      }

      "should save entry(3)" in {
        val saved = kvs.add(fid, "3", data(3))
        saved.map(_.id) shouldBe "3".right
        saved.map(_.data) shouldBe data(3).right
        kvs.fd.length(fid) shouldBe 3L.right
      }

      "should not save entry(2) again" in {
        kvs.add(fid, "2", data(2)).fold(identity, identity) shouldBe EntryExists(s"${fid}.2")
        kvs.fd.length(fid) shouldBe 3L.right
      }

      "should get 3 values from feed" in {
        kvs.all(fid).map(_.toList) shouldBe List(En("3", "2".just, data(3)).right, En("2", "1".just, data(2)).right, En("1", Nothing, data(1)).right).right
      }

      "should remove unexisting entry from feed without error" in {
        kvs.remove(fid, "5") shouldBe Right(Nothing)
      }

      "should remove entry(2) from feed without prev/next/data" in {
        val deleted = kvs.remove(fid, "2")
        deleted.map(_.map(_.id)) shouldBe "2".just.right
        deleted.map(_.map(_.data)) shouldBe data(2).just.right
      }

      "should get 2 values from feed" in {
        kvs.fd.length(fid) shouldBe 2L.right

        val stream = kvs.all(fid)
        stream.map(_.toList) shouldBe List(En("3", "2".just, data(3)).right, En("1", Nothing, data(1)).right).right
      }

      "should remove entry(1) from feed" in {
        val deleted = kvs.remove(fid, "1")
        deleted.map(_.map(_.id)) shouldBe "1".just.right
        deleted.map(_.map(_.data)) shouldBe data(1).just.right
      }

      "should get 1 values from feed" in {
        kvs.fd.length(fid) shouldBe 1L.right

        val stream = kvs.all(fid)
        stream.map(_.toList) shouldBe List(En("3", "2".just, data(3)).right).right
      }

      "should remove entry(3) from feed" in {
        val deleted = kvs.remove(fid, "3")
        deleted.map(_.map(_.id)) shouldBe "3".just.right
        deleted.map(_.map(_.data)) shouldBe data(3).just.right
      }

      "should be empty at the end test" - {
        "length is 0" in { kvs.fd.length(fid) shouldBe 0L.right }
        "all is empty" in { kvs.all(fid) shouldBe LazyList.empty.right }
        "delete fd is ok" in { kvs.fd.delete(fid) shouldBe Right(()) }
        "delete is idempotent" in { kvs.fd.delete(fid) shouldBe Right(()) }
        "all is empty on absent feed" in { kvs.all(fid) shouldBe LazyList.empty.right }
      }
    }

    "no 2" - {
      val fid = "fid2" + java.util.UUID.randomUUID.toString
      
      "should not create stack overflow" in {
        val limit = 100L
        LazyList.from(start=1, step=1).takeWhile(_ <= limit).foreach{ n =>
          val added = kvs.add(fid, data(n))
          added.map(_.id) shouldBe n.toString.right
          added.map(_.data) shouldBe data(n).right
        }
        LazyList.from(start=1, step=1).takeWhile(_ <= limit).foreach{ n =>
          val removed = kvs.remove(fid, n.toString)
          removed.map(_.map(_.id)) shouldBe n.toString.just.right
          removed.map(_.map(_.data)) shouldBe data(n).just.right
          kvs.fd.length(fid) shouldBe (limit-n).right
        }
      }
    }

    "no 3" - {
      val fid = "fid3" + java.util.UUID.randomUUID.toString
      val d = data(0)
      "first auto id is 1" in {
        val s = kvs.add(fid, d)
        s.map(_.id) shouldBe "1".right
      }
      "second auto id is 2" in {
        val s = kvs.add(fid, d)
        s.map(_.id) shouldBe "2".right
      }
      "insert id 5" in {
        val s = kvs.add(fid, "5", d)
        s.map(_.id) shouldBe "5".right
      }
      "next auto id is 6" in {
        val s = kvs.add(fid, d)
        s.map(_.id) shouldBe "6".right
      }
      "insert id 'a'" in {
        val s = kvs.add(fid, "a", d)
        s.map(_.id) shouldBe "a".right
      }
      "next auto id is 7" in {
        val s = kvs.add(fid, d)
        s.map(_.id) shouldBe "7".right
      }
      "insert id 3" in {
        val s = kvs.add(fid, "3", d)
        s.map(_.id) shouldBe "3".right
      }
      "next auto id is 8" in {
        val s = kvs.add(fid, d)
        s.map(_.id) shouldBe "8".right
      }
    }

    "no 4" - {
      val fid = "fid3" + java.util.UUID.randomUUID.toString
      val d = data(0)
      // - - + + - - + + - -
      // 0 9 8 7 6 5 4 3 2 1
      "insert five entries" in {
        1.to(10).map(i => kvs.add(fid, i.toString, d))
      }
      "remove entries" in {
        Seq(1, 2, 5, 6, 9, 10).foreach(i => kvs.remove(fid, i.toString))
      }
      "cleanup removed entries" in {
        kvs.cleanup(fid) shouldBe ().right
      }
      "entries are deleted" in {
        kvs.all(fid).map(_.toList).toString shouldBe List(
          En("8", "7".just, d).right
        , En("7", "4".just, d).right
        , En("4", "3".just, d).right
        , En("3",  Nothing, d).right
        ).right.toString
      }
    }
  }
}
