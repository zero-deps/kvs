package mws.rng

import mws.rng.data.{Data}
import scalaz._

package object store {
  sealed trait PutStatus
  final case object Saved extends PutStatus
  final case class Conflict(broken: Seq[Data]) extends PutStatus

  implicit val PutStatusEqual: Equal[PutStatus] = Equal.equalA
}
