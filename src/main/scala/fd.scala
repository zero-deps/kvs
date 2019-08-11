package zd.kvs
package en

import zd.kvs.store.Dba
import zd.gs.z._

final case class Fd(id: String, top: String = empty, count: Int = 0)

trait FdHandler {
  def pickle(e: Fd): Res[Array[Byte]]
  def unpickle(a: Array[Byte]): Res[Fd]

  def put(el: Fd)(implicit dba: Dba): Res[Fd] = pickle(el).flatMap(x => dba.put(el.id,x)).flatMap(unpickle)
  def get(el: Fd)(implicit dba: Dba): Res[Fd] = dba.get(el.id).fold(
    l => l match {
      case NotFound(k) => FeedNotExists(k).left
      case x => x.left
    },
    r => unpickle(r),
  )
  def delete(el: Fd)(implicit dba: Dba): Res[Fd] = dba.delete(el.id).flatMap(unpickle)
}
