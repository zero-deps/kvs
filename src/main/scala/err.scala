package mws.kvs

sealed trait Err

final case class EntryExist(key: String) extends Err
final case class NotFound(key: String) extends Err
final case class FeedNotExists(key: String) extends Err
final case class FileNotExists(dir: String, name: String) extends Err
final case class FileAlreadyExists(dir: String, name: String) extends Err
final case class Fail(r: String) extends Err
final case class Failed(t: Throwable) extends Err
final case class PickleFail(r: String) extends Err
final case class UnpickleFail(r: String) extends Err

case object RngAskQuorumFailed extends Err
case object RngAskTimeoutFailed extends Err
final case class RngThrow(t: Throwable) extends Err
