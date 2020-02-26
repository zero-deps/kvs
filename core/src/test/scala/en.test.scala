package zd.kvs
package en

import akka.actor.ActorSystem
import akka.testkit._
import com.typesafe.config.ConfigFactory
import org.scalatest._
import org.scalatest.freespec.AnyFreeSpecLike
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try
import zd.gs.z._
import zd.proto.Bytes

class FeedSpec extends TestKit(ActorSystem("Test", ConfigFactory.parseString(conf.tmpl(port=4012))))
  with AnyFreeSpecLike with Matchers with BeforeAndAfterAll {

  def data(n: Int): Bytes = Bytes.unsafeWrap(s"val=${n}".getBytes)

  var kvs: Kvs = null
  override def beforeAll = {
    kvs = Kvs(system)
    Try(Await.result(kvs.onReady, FiniteDuration(1, MINUTES)))
  }
  override def afterAll = TestKit.shutdownActorSystem(system)
  
  def stob(x: String): Bytes = Bytes.unsafeWrap(x.getBytes)

  "feed" - {
    "no 1" - {
      val fid = stob("fid1" + java.util.UUID.randomUUID.toString)
      
      "should be empty at creation" in {
        kvs.all(fid) shouldBe (Right(LazyList.empty))
      }

      "should save e1" in {
        val saved = kvs.add(fid, stob("1"), data(1))
        saved.map(_.data) shouldBe data(1).right
        kvs.fd.length(fid) shouldBe 1L.right
      }

      "should save e2" in {
        val saved = kvs.add(fid, stob("2"), data(2))
        saved.map(_.data) shouldBe data(2).right
        kvs.fd.length(fid) shouldBe 2L.right
      }

      "should get e1 and e2 from feed" in {
        kvs.fd.length(fid) shouldBe 2L.right 
        kvs.all(fid).map(_.toList) shouldBe List(IdEn(id=stob("2"), en=En(stob("1").just, data(2))).right, IdEn(id=stob("1"), en=En(Nothing, data(1))).right).right
      }

      "should save entry(3)" in {
        val saved = kvs.add(fid, stob("3"), data(3))
        saved.map(_.data) shouldBe data(3).right
        kvs.fd.length(fid) shouldBe 3L.right
      }

      "should not save entry(2) again" in {
        kvs.add(fid, stob("2"), data(2)).fold(identity, identity) shouldBe EntryExists(fid, stob("2"))
        kvs.fd.length(fid) shouldBe 3L.right
      }

      "should get 3 values from feed" in {
        kvs.all(fid).map(_.toList) shouldBe List(IdEn(id=stob("3"), en=En(stob("2").just, data(3))).right, IdEn(id=stob("2"), en=En(stob("1").just, data(2))).right, IdEn(id=stob("1"), en=En(Nothing, data(1))).right).right
      }

      "should remove unexisting entry from feed without error" in {
        kvs.remove(fid, stob("5")) shouldBe Right(Nothing)
      }

      "should remove entry(2) from feed without prev/next/data" in {
        val deleted = kvs.remove(fid, stob("2"))
        deleted.map(_.map(_.data)) shouldBe data(2).just.right
      }

      "should get 2 values from feed" in {
        kvs.fd.length(fid) shouldBe 2L.right

        val stream = kvs.all(fid)
        stream.map(_.toList) shouldBe List(IdEn(id=stob("3"), en=En(stob("2").just, data(3))).right, IdEn(id=stob("1"), en=En(Nothing, data(1))).right).right
      }

      "should remove entry(1) from feed" in {
        val deleted = kvs.remove(fid, stob("1"))
        deleted.map(_.map(_.data)) shouldBe data(1).just.right
      }

      "should get 1 values from feed" in {
        kvs.fd.length(fid) shouldBe 1L.right

        val stream = kvs.all(fid)
        stream.map(_.toList) shouldBe List(IdEn(id=stob("3"), en=En(stob("2").just, data(3))).right).right
      }

      "should remove entry(3) from feed" in {
        val deleted = kvs.remove(fid, stob("3"))
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
      val fid = stob("fid2" + java.util.UUID.randomUUID.toString)
      
      "should not create stack overflow" in {
        val limit = 100L
        LazyList.from(start=1, step=1).takeWhile(_ <= limit).foreach{ n =>
          val added = kvs.add(fid, Bytes.unsafeWrap(n.toString.getBytes), data(n))
          added.map(_.data) shouldBe data(n).right
        }
        LazyList.from(start=1, step=1).takeWhile(_ <= limit).foreach{ n =>
          val removed = kvs.remove(fid, Bytes.unsafeWrap(n.toString.getBytes))
          removed.map(_.map(_.data)) shouldBe data(n).just.right
          kvs.fd.length(fid) shouldBe (limit-n).right
        }
      }
    }

    "no 3" - {
      val fid = stob("fid3" + java.util.UUID.randomUUID.toString)
      val d = data(0)
      "first auto id" in {
        val s = kvs.add(fid, d)
        s.map(_.id) shouldBe BytesExt.MinValue.right
      }
      "second auto id" in {
        val s = kvs.add(fid, d)
        s.map(_.id) shouldBe BytesExt.MinValue.increment().right
      }
      "insert id 5" in {
        val s = kvs.add(fid, stob("5"), d)
        s.isRight shouldBe true
      }
      "next auto id is 6" in {
        val s = kvs.add(fid, d)
        s.map(_.id) shouldBe stob("6").right
      }
      "insert id 'a'" in {
        val s = kvs.add(fid, stob("a"), d)
        s.isRight shouldBe true
      }
      "next auto id is 'b'" in {
        val s = kvs.add(fid, d)
        s.map(_.id) shouldBe stob("b").right
      }
      "insert id 3" in {
        val s = kvs.add(fid, stob("3"), d)
        s.isRight shouldBe true
      }
      "next auto id is 'c'" in {
        val s = kvs.add(fid, d)
        s.map(_.id) shouldBe stob("c").right
      }
    }

    "no 4" - {
      val fid = stob("fid4" + java.util.UUID.randomUUID.toString)
      val d = data(0)
      // - - + + - - + + - -
      // 9 8 7 6 5 4 3 2 1 0
      // 56 -> 55 -> 52 -> 51
      "insert five entries" in {
        0.to(9).foreach(i => kvs.add(fid, stob(i.toString), d))
      }
      "maxid is 9" in {
        kvs.fd.get(fid).map(_.map(_.maxid)) shouldBe stob("9").just.right
      }
      "remove entries" in {
        Seq(0, 1, 4, 5, 8, 9).foreach(i => kvs.remove(fid, stob(i.toString)))
      }
      "maxid is 10 nevertheless" in {
        kvs.fd.get(fid).map(_.map(_.maxid)) shouldBe stob("9").just.right
      }
      "cleanup removed entries" in {
        kvs.cleanup(fid) shouldBe ().right
      }
      "entries are deleted" in {
        kvs.all(fid).map(_.toList) shouldBe List(
          IdEn(id=stob("7"), en=En(next=stob("6").just, d)).right
        , IdEn(id=stob("6"), en=En(next=stob("3").just, d)).right
        , IdEn(id=stob("3"), en=En(next=stob("2").just, d)).right
        , IdEn(id=stob("2"), en=En(next=Nothing, d)).right
        ).right
      }
      "maxid is reverted to 7" in {
        kvs.fd.get(fid).map(_.map(_.maxid)) shouldBe stob("7").just.right
      }
      "fix feed doesn't break it" in {
        val res = kvs.fix(fid)
        res.map(_._1._1) shouldBe res.map(_._1._2)
        res.map(_._2._1) shouldBe res.map(_._2._2)
        res.map(_._3._1) shouldBe res.map(_._3._2)
      }
    }

    "no 5" - {
      val fid = stob("fid5" + java.util.UUID.randomUUID.toString)
      implicit def kvs1 = kvs
      "new syntax for prepend" - {
        "without id" in {
          val res = data(0) +: FdId(fid)
          res.map(_.id) shouldBe BytesExt.MinValue.right
        }
        "with id" in {
          val res = (stob("3") -> data(0)) +: FdId(fid)
          res.isRight shouldBe true
        }
      }
    }
  }
}
