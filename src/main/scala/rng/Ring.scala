package mws.rng

import akka.actor.ActorSystem

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object RingApp extends App {
  val system = ActorSystem("rng")
  HashRing(system)
  sys.addShutdownHook {
    system.terminate()
    Await.result(system.whenTerminated, Duration.Inf)
  }
}
