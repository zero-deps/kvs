package kvs

import akka.actor.ActorSystem
import akka.testkit._
import org.scalatest.{Entry=>_,_}
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.should.Matchers
import zero.ext._, either._, option._, traverse._
import zd.proto.Bytes

class ArraySpec extends TestKit(ActorSystem("ArraySpec"))
  with AnyFreeSpecLike with Matchers with BeforeAndAfterAll {

  implicit val dba = store.Mem()
  val fid = FdKey(Bytes.unsafeWrap(Array[Byte](1)))
  def data(b: Byte): Bytes = Bytes.unsafeWrap(Array(b))

  "add 1" in {
    array.add(fid, 3, data(1)) shouldBe ().right
    array.get(fid)(1) shouldBe data(1).some.right
    array.all(fid).flatMap(_.sequence) shouldBe Seq(data(1)).right
  }
  "add 2" in {
    array.add(fid, 3, data(2)) shouldBe ().right
    array.all(fid).flatMap(_.sequence) shouldBe Seq(data(1), data(2)).right
  }
  "add 3" in {
    array.add(fid, 3, data(3)) shouldBe ().right
    array.all(fid).flatMap(_.sequence) shouldBe Seq(data(1), data(2), data(3)).right
  }
  "add 4" in {
    array.add(fid, 3, data(4)) shouldBe ().right
    array.all(fid).flatMap(_.sequence) shouldBe Seq(data(2), data(3), data(4)).right
  }
}
