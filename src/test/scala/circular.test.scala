package kvs

import akka.actor.ActorSystem
import akka.testkit._
import org.scalatest.{Entry=>_,_}
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.should.Matchers
import zero.ext._, either._, option._, traverse._
import zd.proto.Bytes

class CircularSpec extends TestKit(ActorSystem("CircularSpec"))
  with AnyFreeSpecLike with Matchers with BeforeAndAfterAll {

  implicit val dba = store.Mem()
  def data(b: Byte): Bytes = Bytes.unsafeWrap(Array(b))

  "add spec" - {
    val fid = FdKey(Bytes.unsafeWrap(Array[Byte](1)))
    "add 1" in {
      circular.all(fid).flatMap(_.sequence) shouldBe Nil.right
      circular.add(fid, 3, data(1)) shouldBe ().right
      circular.get(fid)(1) shouldBe data(1).some.right
      circular.all(fid).flatMap(_.sequence) shouldBe Seq(data(1)).right
    }
    "add 2" in {
      circular.add(fid, 3, data(2)) shouldBe ().right
      circular.all(fid).flatMap(_.sequence) shouldBe Seq(data(1), data(2)).right
    }
    "add 3" in {
      circular.add(fid, 3, data(3)) shouldBe ().right
      circular.all(fid).flatMap(_.sequence) shouldBe Seq(data(1), data(2), data(3)).right
    }
    "add 4" in {
      circular.add(fid, 3, data(4)) shouldBe ().right
      circular.all(fid).flatMap(_.sequence) shouldBe Seq(data(2), data(3), data(4)).right
    }
  }
  "put spec" - {
    val fid = FdKey(Bytes.unsafeWrap(Array[Byte](2)))
    "put 1" in {
      circular.all(fid).flatMap(_.sequence) shouldBe Nil.right
      circular.put(fid, 1, data(1)) shouldBe ().right
      circular.all(fid).flatMap(_.sequence) shouldBe Seq(data(1)).right
    }
    "put 2" in {
      circular.put(fid, 2, data(2)) shouldBe ().right
      circular.all(fid).flatMap(_.sequence) shouldBe Seq(data(1), data(2)).right
    }
    "put 3" in {
      circular.put(fid, 1, data(3)) shouldBe ().right
      circular.all(fid).flatMap(_.sequence) shouldBe Seq(data(2), data(3)).right
    }
  }
}
