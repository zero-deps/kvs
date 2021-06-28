package zd.kvs

sealed trait Err derives CanEqual

case class EntryExists(key: String) extends Err
case object KeyNotFound extends Err
case class FileNotExists(dir: String, name: String) extends Err
case class FileAlreadyExists(dir: String, name: String) extends Err
case class Fail(r: String) extends Err
case class Failed(t: Throwable) extends Err
case class InvalidArgument(d: String) extends Err

case class RngAskQuorumFailed(why: String) extends Err
case class RngAskTimeoutFailed(op: String, key: String) extends Err
case class RngFail(m: String) extends Err
