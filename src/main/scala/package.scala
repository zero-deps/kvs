package mws

package object kvs {

  sealed trait Err

  final case class EntryExist(key: String) extends Err
  final case class NotFound(key: String) extends Err
  final case class FeedNotExists(key: String) extends Err
  final case class Failed(t: Throwable) extends Err
  final case class UnpickleFailed(t: Throwable) extends Err

  case object RngAskQuorumFailed extends Err
  case object RngAskTimeoutFailed extends Err
  final case class RngThrow(t: Throwable) extends Err

  import scalaz.\/
  type Res[A] = Err \/ A
}
