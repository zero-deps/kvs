import proto._
import java.util.Arrays
import zio._, stream.ZStream

package object kvs {
  type Res[A] = Either[Err, A]
  type KIO[A] = ZIO[ZEnv, Err, A]
  type KUIO[A] = ZIO[ZEnv, Nothing, A]
  type KStream[A] = ZStream[ZEnv, Err, A]

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
    def from_str(x: String): KUIO[ElKey] = IO.effect(ElKey(Bytes.unsafeWrap(x.getBytes("utf8")))).orDie
  }

  def pickle  [A](e: A)    (implicit c: MessageCodec[A]): KUIO[Bytes] = IO.effectTotal(encodeToBytes[A](e))
  def unpickle[A](a: Bytes)(implicit c: MessageCodec[A]): KUIO[A]     = IO.effect(decode[A](a)).orDie // is defect
}
