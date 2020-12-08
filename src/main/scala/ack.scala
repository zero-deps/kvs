package kvs

import zd.proto.Bytes

sealed trait Ack
case class AckSuccess      (v: Option[Bytes])     extends Ack
case class AckQuorumFailed (why: String)          extends Ack
case class AckTimeoutFailed(op: String, k: Bytes) extends Ack
