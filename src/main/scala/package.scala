import java.util.Arrays
import proto._
import zio._

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
    def from_str(x: String): UIO[ElKey] = IO.effectTotal(ElKey(Bytes.unsafeWrap(x.getBytes("utf8"))))
  }

  def pickle  [A](e: A)    (implicit c: MessageCodec[A]): UIO[Bytes] = IO.effectTotal(encodeToBytes[A](e))
  def unpickle[A](a: Bytes)(implicit c: MessageCodec[A]): UIO[A]     = IO.effect(decode[A](a)).orDie // is defect

  implicit class OptionExt[A](x: Option[A]) {
    def cata[B](f: A => B, b: => B): B = x match {
      case Some(a) => f(a)
      case None => b
    }
  }

  implicit class BooleanExt(x: Boolean) {
    def fold[A](t: => A, f: => A): A = if (x) t else f
  }
}
