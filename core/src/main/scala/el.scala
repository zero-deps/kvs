package kvs
package el

import zd.proto.Bytes

import store.Dba

object ElHandler {
  def put(k: ElKey, v: Bytes)(implicit dba: Dba): Res[Unit] = dba.put(k, v)
  def get(k: ElKey)(implicit dba: Dba): Res[Option[Bytes]] = dba.get(k)
  def delete(k: ElKey)(implicit dba: Dba): Res[Unit] = dba.delete(k)
}
