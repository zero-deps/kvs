import java.util.Arrays
import proto._
import zio.{ZIO, URIO, IO}
import zio.blocking.Blocking

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
  }

  object ElKeyExt {
    val MinValue = ElKey(Bytes.unsafeWrap(Array(Byte.MinValue)))
    val EmptyValue = ElKey(Bytes.empty)
    def from_str(x: String): URIO[Blocking, ElKey] = IO.effect(ElKey(Bytes.unsafeWrap(x.getBytes("utf8")))).orDie
  }

  def pickle  [A](e: A)    (implicit c: MessageCodec[A]): URIO[Blocking, Bytes] = IO.effectTotal(encodeToBytes[A](e))
  def unpickle[A](a: Bytes)(implicit c: MessageCodec[A]): URIO[Blocking, A]     = IO.effect(decode[A](a)).orDie // is defect
}
