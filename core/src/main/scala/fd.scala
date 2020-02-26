package zd.kvs
package en

import zd.kvs.store.Dba
import zd.gs.z._
import zd.proto.api.{N, MessageCodec}
import zd.proto.macrosapi.{caseCodecAuto}
import zd.proto.Bytes

final case class Fd
  ( @N(1) id: Bytes
  , @N(2) head: Option[Bytes]
  , @N(3) length: Long
  , @N(4) removed: Long
  , @N(5) maxid: Bytes
  )
object Fd {
  def apply(id: Bytes, head: Option[Bytes]=None, length: Long=0, removed: Long=0, maxid: Bytes=BytesExt.Empty): Fd = {
    new Fd(id=id, head=head, length=length, removed=removed, maxid=maxid)
  }
}

final case class FdId(id: Bytes)

object FdHandler {
  private implicit val codec: MessageCodec[Fd] = caseCodecAuto[Fd]

  def put(el: Fd)(implicit dba: Dba): Res[Unit] = dba.put(el.id, pickle(el))

  def get(id: Bytes)(implicit dba: Dba): Res[Option[Fd]] = dba.get(id) match {
    case Right(Some(x)) => unpickle(x).just.right
    case Right(None) => Nothing.right
    case x@Left(_) => x.coerceRight
  }

  def length(id: Bytes)(implicit dba: Dba): Res[Long] = get(id).map(_.map(_.length).getOrElse(0L))

  def delete(id: Bytes)(implicit dba: Dba): Res[Unit] = dba.delete(id)
}
