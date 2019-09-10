package zd

import zd.proto.api.{MessageCodec, encode, decode}
import scala.util.Try
import zd.gs.z._
import zd.proto.Bytes

package object kvs {
  type Res[A] = Either[Err, A]

  implicit class BytesExt(x: Bytes) {
    def splitAt(n: Int): (Bytes, Bytes) = {
      val res = x.unsafeArray.splitAt(n)
      (Bytes.unsafeWrap(res._1), Bytes.unsafeWrap(res._2))
    }
    def length: Int = x.unsafeArray.length
  }

  implicit class FdIdExt(fd: en.FdId) {
    def +:(data: Bytes)(implicit kvs: Kvs): Res[en.En] = en.EnHandler.prepend(fd.id, data)(kvs.dba)
    def +:(id_data: (Bytes, Bytes))(implicit kvs: Kvs): Res[en.En] = en.EnHandler.prepend(fd.id, id_data._1, id_data._2)(kvs.dba)
  }

  def pickle[A](e: A)(implicit c: MessageCodec[A]): Bytes = Bytes.unsafeWrap(encode[A](e))
  def unpickle[A](a: Bytes)(implicit c: MessageCodec[A]): Res[A] = Try(decode[A](a.unsafeArray)).fold(Throwed(_).left, _.right)
}
