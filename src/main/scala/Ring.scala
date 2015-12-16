package mws.rng

import akka.actor._
import akka.kernel.Bootable
import com.typesafe.config.ConfigFactory

sealed trait Ack
case object AckSuccess extends Ack
case object AckQuorumFailed extends Ack
case object AckTimeoutFailed extends Ack

class RingApp extends Bootable {
  implicit val system = ActorSystem("rng", ConfigFactory.load)

  override def startup(): Unit = HashRing(system)
  override def shutdown():Unit = system.shutdown
}
