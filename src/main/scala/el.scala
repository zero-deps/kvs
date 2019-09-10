package zd.kvs
package el

import zd.proto.Bytes
import zd.kvs.store.Dba

object ElHandler {
  def put(k: Bytes, v: Bytes)(implicit dba: Dba): Res[Unit] = dba.put(k, v)
  def get(k: Bytes)(implicit dba: Dba): Res[Option[Bytes]] = dba.get(k)
  def delete(k: Bytes)(implicit dba: Dba): Res[Unit] = dba.delete(k)
}
