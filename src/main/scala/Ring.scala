package mws.rng

import akka.actor.ActorSystem

object RingApp extends App {
  val system = ActorSystem("rng")
  HashRing(system)
  sys.addShutdownHook {
    system.shutdown()
    system.awaitTermination()
  }
}
