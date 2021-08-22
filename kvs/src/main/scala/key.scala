package kvs

import proto.*

sealed trait Key

@N(1) final case class ElKey(@N(1) bytes: Array[Byte]) extends Key
@N(2) final case class FdKey(@N(1) bytes: Array[Byte]) extends Key
@N(3) final case class EnKey(@N(1) fid: FdKey, @N(2) id: ElKey) extends Key

@N(4) final case class PathKey(@N(1) dir: FdKey, @N(2) name: ElKey) extends Key
@N(5) final case class ChunkKey(@N(1) path: PathKey, @N(2) idx: Long) extends Key
