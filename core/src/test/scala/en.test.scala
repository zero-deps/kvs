package kvs
package en

import akka.actor.ActorSystem
import akka.testkit._
import org.scalatest.{Entry=>_,_}
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.should.Matchers
import zero.ext._, either._, option._
import zd.proto.Bytes

class FeedSpec extends TestKit(ActorSystem("FeedSpec"))
  with AnyFreeSpecLike with Matchers with BeforeAndAfterAll {

  val kvs = Kvs.mem()
  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  val key1 = keyN(1)
  val key2 = keyN(2)
  val key3 = keyN(3)
  def keyN(n: Byte): ElKey = ElKey(Bytes.unsafeWrap(Array(n)))
  def data(n: Byte): Bytes = Bytes.unsafeWrap(Array(n))

  "feed" - {
    "no 1" - {
      val fid: FdKey = FdKey(data(1))
      implicit object Entry1 extends DataCodec[Int] {
        def extract(xs: Bytes): Int = xs.mkString.toInt
        def insert(x: Int): Bytes = Bytes.unsafeWrap(x.toString.getBytes)
      }
      "should be empty at creation" in {
        kvs.all[Int](fid) shouldBe LazyList.empty.right
      }

      "should save e1" in {
        val saved = kvs.add[Int](fid, key1, 1)
        saved shouldBe ().right
        kvs.fd.length(fid) shouldBe 1L.right
      }

      "should save e2" in {
        val saved = kvs.add[Int](fid, key2, 2)
        saved shouldBe ().right
        kvs.fd.length(fid) shouldBe 2L.right
      }

      "should get e1 and e2 from feed" in {
        kvs.fd.length(fid) shouldBe 2L.right 
        kvs.all[Int](fid).map(_.toList) shouldBe List((key2 -> 2).right, (key1 -> 1).right).right
      }

      "should save entry(3)" in {
        val saved = kvs.add[Int](fid, key3, 3)
        saved shouldBe ().right
        kvs.fd.length(fid) shouldBe 3L.right
      }

      "should not save entry(2) again" in {
        kvs.add[Int](fid, key2, 2).fold(identity, identity) shouldBe EntryExists(EnKey(fid, key2))
        kvs.fd.length(fid) shouldBe 3L.right
      }

      "should get 3 values from feed" in {
        kvs.all[Int](fid).map(_.toList) shouldBe List((key3 -> 3).right, (key2 -> 2).right, (key1 -> 1).right).right
      }

      "should remove unexisting entry from feed without error" in {
        kvs.remove[Int](fid, keyN(5)) shouldBe Right(None)
      }

      "should remove entry(2)" in {
        val deleted = kvs.remove[Int](fid, key2)
        deleted shouldBe 2.some.right
      }

      "should get 2 values from feed" in {
        kvs.fd.length(fid) shouldBe 2L.right

        val stream = kvs.all[Int](fid)
        stream.map(_.toList) shouldBe List((key3 -> 3).right, (key1 -> 1).right).right
      }

      "should remove entry(1) from feed" in {
        val deleted = kvs.remove[Int](fid, key1)
        deleted shouldBe 1.some.right
      }

      "should get 1 values from feed" in {
        kvs.fd.length(fid) shouldBe 1L.right

        val stream = kvs.all[Int](fid)
        stream.map(_.toList) shouldBe List((key3 -> 3).right).right
      }

      "should remove entry(3) from feed" in {
        val deleted = kvs.remove[Int](fid, key3)
        deleted shouldBe 3.some.right
      }

      "should be empty at the end test" - {
        "length is 0" in { kvs.fd.length(fid) shouldBe 0L.right }
        "all is empty" in { kvs.all[Int](fid) shouldBe LazyList.empty.right }
        "delete fd is ok" in { kvs.fd.delete(fid) shouldBe Right(()) }
        "delete is idempotent" in { kvs.fd.delete(fid) shouldBe Right(()) }
        "all is empty on absent feed" in { kvs.all[Int](fid) shouldBe LazyList.empty.right }
      }
    }

    "no 2" - {
      val fid: FdKey = FdKey(data(2))
      implicit object Entry1 extends DataCodec[Int] {
        def extract(xs: Bytes): Int = xs.mkString.toInt
        def insert(x: Int): Bytes = Bytes.unsafeWrap(x.toString.getBytes)
      }
      "first auto id" in {
        val s = kvs.add[Int](fid, 0)
        s shouldBe ElKeyExt.MinValue.right
      }
      "second auto id" in {
        val s = kvs.add[Int](fid, 0)
        s shouldBe ElKeyExt.MinValue.increment().right
      }
      "insert id 5" in {
        val s = kvs.add[Int](fid, keyN(5), 0)
        s.isRight shouldBe true
      }
      "next auto id is 6" in {
        val s = kvs.add[Int](fid, 0)
        s shouldBe keyN(6).right
      }
      "insert id 'a'" in {
        val s = kvs.add[Int](fid, keyN('a'), 0)
        s.isRight shouldBe true
      }
      "next auto id is 'b'" in {
        val s = kvs.add[Int](fid, 0)
        s shouldBe keyN('b').right
      }
      "insert id 3" in {
        val s = kvs.add[Int](fid, key3, 0)
        s.isRight shouldBe true
      }
      "next auto id is 'c'" in {
        val s = kvs.add[Int](fid, 0)
        s shouldBe keyN('c').right
      }
    }

    "no 3" - {
      val fid: FdKey = FdKey(data(3))
      implicit object Entry1 extends DataCodec[Int] {
        def extract(xs: Bytes): Int = xs.mkString.toInt
        def insert(x: Int): Bytes = Bytes.unsafeWrap(x.toString.getBytes)
      }
      // - - + + - - + + - -
      // 9 8 7 6 5 4 3 2 1 0
      // 56 -> 55 -> 52 -> 51
      "insert nine entries" in {
        0.to(9).foreach(i => kvs.add[Int](fid, keyN(i.toByte), 0))
      }
      "maxid is 9" in {
        kvs.fd.get(fid).map(_.map(_.maxid)) shouldBe keyN(9).some.right
      }
      "remove entries" in {
        Seq(0, 1, 4, 5, 8, 9).foreach(i => kvs.remove[Int](fid, keyN(i.toByte)))
      }
      "maxid is 10 nevertheless" in {
        kvs.fd.get(fid).map(_.map(_.maxid)) shouldBe keyN(9).some.right
      }
      "cleanup removed entries" in {
        kvs.cleanup(fid) shouldBe ().right
      }
      "entries are deleted" in {
        kvs.all[Int](fid).map(_.toList) shouldBe List(
          (keyN(7) -> 0).right
        , (keyN(6) -> 0).right
        , (keyN(3) -> 0).right
        , (keyN(2) -> 0).right
        ).right
      }
      "maxid is reverted to 7" in {
        kvs.fd.get(fid).map(_.map(_.maxid)) shouldBe keyN(7).some.right
      }
      "fix feed doesn't break it" in {
        val res = kvs.fix(fid)
        res.map(_._1._1) shouldBe res.map(_._1._2)
        res.map(_._2._1) shouldBe res.map(_._2._2)
        res.map(_._3._1) shouldBe res.map(_._3._2)
      }
    }
  }
}
