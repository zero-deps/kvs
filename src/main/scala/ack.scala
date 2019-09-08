package zd.kvs

sealed trait Err

final case class EntryExists(fid: Bytes, id: Bytes) extends Err

final case class FileNotExists(dir: Bytes, name: Bytes) extends Err
final case class FileAlreadyExists(dir: Bytes, name: Bytes) extends Err

final case class Fail(r: String) extends Err
final case class Throwed(x: Throwable) extends Err

sealed trait Ack
final case class AckSuccess(v: Option[Bytes]) extends Ack
final case class AckQuorumFailed(why: String) extends Ack with Err
final case class AckTimeoutFailed(on: String) extends Ack with Err
