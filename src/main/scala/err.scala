package zd.kvs

sealed trait Err

final case class EntryExists(key: String) extends Err
final case class FileNotExists(dir: String, name: String) extends Err
final case class FileAlreadyExists(dir: String, name: String) extends Err
final case class Fail(r: String) extends Err
final case class Throwed(x: Throwable) extends Err

final case class RngAskQuorumFailed(why: String) extends Err
final case class RngAskTimeoutFailed(on: String) extends Err
final case class RngThrow(t: Throwable) extends Err
final case class RngFail(m: String) extends Err
