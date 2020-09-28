package zd.kvs

import zd.proto.Bytes

sealed trait Err

final case class EntryExists(key: EnKey) extends Err

final case class FileNotExists(path: PathKey) extends Err
final case class FileAlreadyExists(path: PathKey) extends Err

final case class Fail(r: String) extends Err
final case class Throwed(x: Throwable) extends Err

sealed trait Ack
final case class AckSuccess(v: Option[Bytes]) extends Ack
final case class AckQuorumFailed(why: String) extends Ack with Err
final case class AckTimeoutFailed(op: String, k: Bytes) extends Ack with Err
