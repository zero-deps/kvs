package kvs

import proto.Bytes
import store.Dba
import zio.IO

object el {
  def put(k: ElKey, v: Bytes)(implicit dba: Dba): IO[Err, Unit]          = dba.put(k, v)
  def get(k: ElKey          )(implicit dba: Dba): IO[Err, Option[Bytes]] = dba.get(k   )
  def del(k: ElKey          )(implicit dba: Dba): IO[Err, Unit]          = dba.del(k   )
}
