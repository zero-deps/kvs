package kvs
package en

import zero.ext._, either._, option._
import zd.proto._, api._, macrosapi._

import store.Dba

final case class Fd
  ( @N(1) head: Option[ElKey]
  , @N(2) length: Long
  , @N(3) removed: Long
  , @N(4) maxid: ElKey
  )
object Fd {
  def apply(head: Option[ElKey]=None, length: Long=0, removed: Long=0, maxid: ElKey=ElKey(Bytes.empty)): Fd = {
    new Fd(head=head, length=length, removed=removed, maxid=maxid)
  }
}

object FdHandler {
  private implicit val fdc = {
    implicit val elkeyc = caseCodecAuto[ElKey]
    caseCodecAuto[Fd]
  }

  def put(id: FdKey, el: Fd)(implicit dba: Dba): Res[Unit] = dba.put(id, pickle(el))

  def get(id: FdKey)(implicit dba: Dba): Res[Option[Fd]] = dba.get(id) match {
    case Right(Some(x)) => unpickle(x).some.right
    case Right(None) => none.right
    case x@Left(_) => x.coerceRight
  }

  def length(id: FdKey)(implicit dba: Dba): Res[Long] = get(id).map(_.map(_.length).getOrElse(0L))

  def delete(id: FdKey)(implicit dba: Dba): Res[Unit] = dba.delete(id)
}
