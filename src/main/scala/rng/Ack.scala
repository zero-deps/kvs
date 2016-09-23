package mws.rng

sealed trait Ack
case object AckSuccess extends Ack
case object AckQuorumFailed extends Ack
case object AckTimeoutFailed extends Ack
