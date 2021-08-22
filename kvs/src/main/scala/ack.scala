package kvs

sealed trait Ack
case class AckSuccess(v: Option[Array[Byte]]) extends Ack
case class AckQuorumFailed(why: String) extends Ack
case class AckTimeoutFailed(op: String, k: Array[Byte]) extends Ack
