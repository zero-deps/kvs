package zd.kvs
package search

import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest._
import java.util.Arrays

class InTest extends AnyFreeSpecLike with Matchers {
  val in = new BytesIndexInput("test", Vector(
    Array[Byte](1,2,3)
  , Array[Byte](4,5,6)
  , Array[Byte](7,8,9)
  , Array[Byte](0,1,2)
  ))
  "read byte" in {
    in.readByte shouldBe 1
    in.readByte shouldBe 2
    in.readByte shouldBe 3
    in.readByte shouldBe 4
    in.seek(0)
    in.readByte shouldBe 1
    in.seek(2)
    in.readByte shouldBe 3
    in.seek(3)
    in.readByte shouldBe 4
    in.seek(7)
    in.readByte shouldBe 8
    in.seek(5)
    in.readByte shouldBe 6
    in.readByte shouldBe 7
  }
  "read bytes across arrays" in {
    in.seek(4)
    val res_offset = 2
    val res_len = 6
    val res = new Array[Byte](res_offset+res_len)
    in.readBytes(res, res_offset, res_len)
    assert(Arrays.equals(res, Array.fill[Byte](res_offset)(0)++Array[Byte](5,6,7,8,9,0)), "bad: "+res.mkString("[",",","]"))
  }
  "slice" in {
    val slice = in.slice("slice", 4, 3)
    slice.length shouldBe 3
    slice.getFilePointer shouldBe 0
    slice.readByte shouldBe 5
    slice.readByte shouldBe 6
    slice.readByte shouldBe 7
    slice.seek(1)
    slice.readByte shouldBe 6
    slice.seek(0)
    val res = new Array[Byte](3)
    slice.readBytes(res, 0, 3)
    assert(Arrays.equals(res, Array[Byte](5,6,7)), "bad: "+res.mkString("[",",","]"))
  }
}