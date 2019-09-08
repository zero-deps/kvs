package zd

import scala.collection.immutable.ArraySeq
import zd.proto.api.{MessageCodec, encode, decode}
import scala.util.Try
import zd.gs.z._

package object kvs {
  type Res[A] = Either[Err, A]

  type Bytes = ArraySeq[Byte]
  def Bytes(xs: Array[Byte]): Bytes = ArraySeq.unsafeWrapArray[Byte](xs)

  implicit class FdIdExt(fd: en.FdId) {
    def +:(data: Bytes)(implicit kvs: Kvs): Res[en.En] = en.EnHandler.prepend(fd.id, data)(kvs.dba)
    def +:(id_data: (Bytes, Bytes))(implicit kvs: Kvs): Res[en.En] = en.EnHandler.prepend(fd.id, id_data._1, id_data._2)(kvs.dba)
  }

  def pickle[A](e: A)(implicit c: MessageCodec[A]): Bytes = Bytes(encode[A](e))
  def unpickle[A](a: Bytes)(implicit c: MessageCodec[A]): Res[A] = Try(decode[A](a.toArray)).fold(Throwed(_).left, _.right)
}
