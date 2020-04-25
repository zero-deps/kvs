package zd.kvs
package en

import zd.kvs.store.Dba
import zero.ext._, either._, option._
import zd.proto.api.{N, encode, decode}
import zd.proto.macrosapi.caseCodecAuto

final case class Fd
  ( @N(1) id: String
  , @N(2) top: String = empty
  , @N(3) count: Int = 0
  )

trait FdHandler {
  def pickle(e: Fd): Res[Array[Byte]]
  def unpickle(a: Array[Byte]): Res[Fd]

  def put(el: Fd)(implicit dba: Dba): Res[Fd] = pickle(el).flatMap(x => dba.put(el.id,x)).flatMap(unpickle)
  def get(el: Fd)(implicit dba: Dba): Res[Option[Fd]] = dba.get(el.id) match {
    case Right(Some(x)) => unpickle(x).map(_.some)
    case Right(None) => none.right
    case x@Left(_) => x.coerceRight
  }
  def delete(el: Fd)(implicit dba: Dba): Res[Unit] = dba.delete(el.id)
}

object feedHandler extends FdHandler {
  implicit private[this] val codec = caseCodecAuto[Fd]
  def pickle(e: Fd): Res[Array[Byte]] = encode[Fd](e).right
  def unpickle(a: Array[Byte]): Res[Fd] = decode[Fd](a).right
}