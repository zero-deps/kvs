package zd.kvs

import scala.util.Try

import akka.actor.ActorSystem

object Run extends App {
  import com.typesafe.config.ConfigFactory
  val cfg = ConfigFactory.load()
  implicit val system = ActorSystem("kvs", cfg)
  val log = system.log
  val kvs = Kvs(system)

  sys.addShutdownHook {
    println("SHUTDOWN")
    system.terminate()
    import scala.concurrent.Await
    import scala.concurrent.duration.Duration
    val _ = Try(Await.result(system.whenTerminated,Duration.Inf))
  }

  import system.dispatcher

  kvs.onReady.map{ _ =>
  }
}
