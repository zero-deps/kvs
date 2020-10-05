package zero.kvs
package kvszio

import zd.proto.Bytes
import zd.kvs._

sealed trait ShardMsg
final case class ShardAdd(fid: FdKey, id: ElKey, data: Bytes) extends ShardMsg
final case class ShardPut(fid: FdKey, id: ElKey, data: Bytes) extends ShardMsg
final case class ShardPutBulk(fid: FdKey, a: Vector[(Bytes, Bytes)]) extends ShardMsg
final case class ShardRemove(fid: FdKey, id: ElKey) extends ShardMsg
