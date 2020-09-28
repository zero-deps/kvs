package zd.kvs
package en

import zd.kvs.store.Dba
import zero.ext._, either._, option._
import zd.proto.api.{N, MessageCodec}
import zd.proto.macrosapi.caseCodecAuto
import zd.proto.Bytes

final case class Fd
  ( @N(1) head: Option[Bytes]
  , @N(2) length: Long
  , @N(3) removed: Long
  , @N(4) maxid: Bytes
  )
object Fd {
  def apply(head: Option[Bytes]=None, length: Long=0, removed: Long=0, maxid: Bytes=BytesExt.Empty): Fd = {
    new Fd(head=head, length=length, removed=removed, maxid=maxid)
  }
}

object FdHandler {
  private implicit val codec: MessageCodec[Fd] = caseCodecAuto[Fd]

  def put(id: FdKey, el: Fd)(implicit dba: Dba): Res[Unit] = dba.put(id, pickle(el))

  def get(id: FdKey)(implicit dba: Dba): Res[Option[Fd]] = dba.get(id) match {
    case Right(Some(x)) => unpickle(x).some.right
    case Right(None) => none.right
    case x@Left(_) => x.coerceRight
  }

  def length(id: FdKey)(implicit dba: Dba): Res[Long] = get(id).map(_.map(_.length).getOrElse(0L))

  def delete(id: FdKey)(implicit dba: Dba): Res[Unit] = dba.delete(id)
}
