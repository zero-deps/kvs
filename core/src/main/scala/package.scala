import zd.proto.api._
import zd.proto.Bytes
import java.util.Arrays

package object kvs {
  type Res[A] = Either[Err, A]

  implicit class ElKeyExt(key: ElKey) {
    import ElKeyExt._
    def increment(): ElKey = {
      val x = key.bytes
      val len = x.length
      if (len == 0) MinValue
      else {
        val last = x.unsafeArray(len-1)
        if (last == Byte.MaxValue) {
          val ext = Arrays.copyOf(x.unsafeArray, len+1)
          ext(len) = Byte.MinValue
          ElKey(Bytes.unsafeWrap(ext))
        } else {
          val ext: Array[Byte] = Arrays.copyOf(x.unsafeArray, len)
          ext(len-1) = (ext(len-1) + 1).toByte
          ElKey(Bytes.unsafeWrap(ext))
        }
      }
    }
    def decrement(): ElKey = {
      val x = key.bytes
      val len = x.length
      if (len == 0) EmptyValue
      else {
        val last = x.unsafeArray(len-1)
        if (last == Byte.MinValue) {
          if (len == 1) EmptyValue
          else {
            val ext = Arrays.copyOf(x.unsafeArray, len-1)
            ElKey(Bytes.unsafeWrap(ext))
          }
        } else {
          val ext: Array[Byte] = Arrays.copyOf(x.unsafeArray, len)
          ext(len-1) = (ext(len-1) - 1).toByte
          ElKey(Bytes.unsafeWrap(ext))
        }
      }
    }
  }

  object ElKeyExt {
    val MinValue = ElKey(Bytes.unsafeWrap(Array(Byte.MinValue)))
    val EmptyValue = ElKey(Bytes.empty)
    def from_str(x: String): ElKey = ElKey(Bytes.unsafeWrap(x.getBytes("utf8")))
  }

  def pickle[A](e: A)(implicit c: MessageCodec[A]): Bytes = encodeToBytes[A](e)
  def unpickle[A](a: Bytes)(implicit c: MessageCodec[A]): A = decode[A](a)
}
