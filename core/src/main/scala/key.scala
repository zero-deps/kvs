package zd.kvs

import zd.proto._, api._

sealed trait Key

@N(1) final case class ElKey(@N(1) name: Bytes) extends Key
@N(2) final case class FdKey(@N(1) name: Bytes) extends Key
@N(3) final case class EnKey(@N(1) fid: FdKey, @N(2) id: Bytes) extends Key

@N(4) final case class PathKey(@N(1) dir: FdKey, @N(2) name: Bytes) extends Key
@N(5) final case class ChunkKey(@N(1) path: PathKey, @N(2) idx: Int) extends Key
