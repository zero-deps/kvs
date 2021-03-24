package kvs
package search

import zio.test._, Assertion._
import proto.Bytes

object SearchInSpec extends DefaultRunnableSpec {
  def makein = new BytesIndexInput("test", Vector(
    Bytes.unsafeWrap(Array[Byte](1,2,3))
  , Bytes.unsafeWrap(Array[Byte](4,5,6))
  , Bytes.unsafeWrap(Array[Byte](7,8,9))
  , Bytes.unsafeWrap(Array[Byte](0,1,2))
  ))

  def spec = suite("MergeSpec")(
    test("read bytes") {
      val in = makein
      (1 to 3).foreach(_ => in.readByte)
      assert(in.readByte)(equalTo(4.toByte))
    }
  , test("seek to 0") {
      val in = makein
      in.readByte
      in.seek(0)
      assert(in.readByte)(equalTo(1.toByte))
    }
  , test("seek to 2") {
      val in = makein
      in.seek(2)
      assert(in.readByte)(equalTo(3.toByte))
    }
  , test("seek to 3") {
      val in = makein
      in.seek(3)
      assert(in.readByte)(equalTo(4.toByte))
    }
  , test("seek to 7") {
      val in = makein
      in.seek(7)
      assert(in.readByte)(equalTo(8.toByte))
    }
  , test("seek to 5") {
      val in = makein
      in.seek(5)
      assert(in.readByte)(equalTo(6.toByte)) &&
      assert(in.readByte)(equalTo(7.toByte))
    }
  , test("read bytes across arrays") {
      val in = makein
      in.seek(4)
      val res_offset = 2
      val res_len = 6
      val res = new Array[Byte](res_offset+res_len)
      in.readBytes(res, res_offset, res_len)
      assert(res)(equalTo(Array[Byte](0,0,5,6,7,8,9,0)))
    }
  , test("slice") {
      val in = makein
      val slice = in.slice("slice", 4, 3)
      val x1 = slice.length
      val x2 = slice.getFilePointer
      val x3 = slice.readByte
      val x4 = slice.readByte
      val x5 = slice.readByte
      slice.seek(1)
      val x6 = slice.readByte
      slice.seek(0)
      val res = new Array[Byte](3)
      slice.readBytes(res, 0, 3)
      assert(x1)(equalTo(3L)) &&
      assert(x2)(equalTo(0L)) &&
      assert(x3)(equalTo(5.toByte)) &&
      assert(x4)(equalTo(6.toByte)) &&
      assert(x5)(equalTo(7.toByte)) &&
      assert(x6)(equalTo(6.toByte)) &&
      assert(res)(equalTo(Array[Byte](5,6,7)))
    }
  )
}
