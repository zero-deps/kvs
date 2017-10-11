package mws

package object kvs {

  sealed trait Err

  final case class EntryExist(key: String) extends Err
  final case class NotFound(key: String) extends Err
  final case class Failed(t: Throwable) extends Err
  final case class UnpickleFailed(t: Throwable) extends Err

  sealed trait ErrRng extends Err
  case object RngAskQuorumFailed extends ErrRng
  case object RngAskTimeoutFailed extends ErrRng
  final case class RngThrow(t: Throwable) extends ErrRng

  import scalaz.\/
  type Res[A] = Err \/ A
}
