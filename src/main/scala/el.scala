package kvs

import zd.proto.Bytes
import store.Dba

object el {
  def put(k: ElKey, v: Bytes)(implicit dba: Dba): KIO[Unit]          = dba.put(k, v)
  def get(k: ElKey          )(implicit dba: Dba): KIO[Option[Bytes]] = dba.get(k   )
  def del(k: ElKey          )(implicit dba: Dba): KIO[Unit]          = dba.del(k   )
}
