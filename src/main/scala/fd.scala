package zd.kvs
package en

import zd.kvs.store.Dba
import zd.gs.z._

final case class Fd(id: String, top_opt: Option[String]=None, count: Int=0)

trait FdHandler {
  def pickle(e: Fd): Res[Array[Byte]]
  def unpickle(a: Array[Byte]): Res[Fd]

  def put(el: Fd)(implicit dba: Dba): Res[Fd] = pickle(el).flatMap(x => dba.put(el.id,x)).flatMap(unpickle)
  def get(el: Fd)(implicit dba: Dba): Res[Option[Fd]] = dba.get(el.id) match {
    case Right(Some(x)) => unpickle(x).map(_.just)
    case Right(None) => Right(None)
    case x@Left(_) => x.coerceRight
  }
  def delete(el: Fd)(implicit dba: Dba): Res[Unit] = dba.delete(el.id)
}
