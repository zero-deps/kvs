package zd.kvs
package el

import zd.kvs.store.Dba
import scala.collection.immutable.ArraySeq

object ElHandler {
  def put(k: String, v: Array[Byte])(implicit dba: Dba): Res[Unit] = dba.put(k, v)
  def get(k: String)(implicit dba: Dba): Res[Option[ArraySeq[Byte]]] = dba.get(k).map(_.map(ArraySeq.unsafeWrapArray(_)))
  def delete(k: String)(implicit dba: Dba): Res[Unit] = dba.delete(k)
}
