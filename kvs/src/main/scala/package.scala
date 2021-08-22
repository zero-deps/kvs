import java.util.Arrays
import proto.*
import zio.*

package object kvs {
  type Res[A] = Either[Err, A]

  implicit class ElKeyExt(key: ElKey) {
    import ElKeyExt.*
    def increment(): ElKey = {
      val x = key.bytes
      val len = x.length
      if (len == 0) MinValue
      else {
        val last = x(len-1)
        if (last == Byte.MaxValue) {
          val ext = Arrays.copyOf(x, len+1)
          ext(len) = Byte.MinValue
          ElKey(ext)
        } else {
          val ext: Array[Byte] = Arrays.copyOf(x, len)
          ext(len-1) = (ext(len-1) + 1).toByte
          ElKey(ext)
        }
      }
    }
  }

  object ElKeyExt {
    val MinValue = ElKey(Array(Byte.MinValue))
    val EmptyValue = ElKey(Array.emptyByteArray)
    def from_str(x: String): UIO[ElKey] = IO.effectTotal(ElKey(x.getBytes("utf8")))
  }

  def pickle[A](e: A)(implicit c: MessageCodec[A]): UIO[Array[Byte]] = IO.effectTotal(encode[A](e))
  def unpickle[A](a: Array[Byte])(implicit c: MessageCodec[A]): UIO[A] = IO.effect(decode[A](a)).orDie // is defect

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
