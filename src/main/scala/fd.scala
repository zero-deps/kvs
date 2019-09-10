package zd.kvs
package en

import zd.kvs.store.Dba
import zd.gs.z._
import zd.proto.api.{N, MessageCodec}
import zd.proto.macrosapi.{caseCodecAuto}
import zd.proto.Bytes

final case class Fd
  ( @N(1) id: Bytes
  , @N(2) head: Option[Bytes]=None
  , @N(3) length: Long=0
  , @N(4) removed: Long=0
  , @N(5) maxid: Long=0
  )

final case class FdId(id: Bytes)

object FdHandler {
  private implicit val codec: MessageCodec[Fd] = caseCodecAuto[Fd]

  def put(el: Fd)(implicit dba: Dba): Res[Unit] = dba.put(el.id, pickle(el))

  def get(id: Bytes)(implicit dba: Dba): Res[Option[Fd]] = dba.get(id) match {
    case Right(Some(x)) => unpickle(x).map(_.just)
    case Right(None) => Right(None)
    case x@Left(_) => x.coerceRight
  }

  def length(id: Bytes)(implicit dba: Dba): Res[Long] = get(id).map(_.map(_.length).getOrElse(0L))

  def delete(id: Bytes)(implicit dba: Dba): Res[Unit] = dba.delete(id)
}
