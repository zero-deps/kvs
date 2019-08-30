package zd.rng

sealed trait Ack
final case class AckSuccess(v: Option[Value]) extends Ack
final case class AckQuorumFailed(why: String) extends Ack
final case class AckTimeoutFailed(on: String) extends Ack
