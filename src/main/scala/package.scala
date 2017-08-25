package mws

package object kvs {

  sealed trait Err

  final case class EntryExist(key: String) extends Err
  final case class NotFound(key: String) extends Err

  sealed trait ErrRng extends Err
  case object RngAskQuorumFailed extends ErrRng
  case object RngAskTimeoutFailed extends ErrRng
  final case class RngThrow(t: Throwable) extends ErrRng

  implicit val ErrShows: scalaz.Show[Err] = scalaz.Show.shows {
    case EntryExist(key) => s"entry ${key} exist"
    case NotFound(key) => s"entry ${key} not found"
    case RngAskQuorumFailed => "ask quorum failed"
    case RngAskTimeoutFailed => "ask timeout failed"
    case RngThrow(t) => s"rng throw: ${t.getMessage}"
  }

  type Res[T] = Either[Err, T]
}
