package mws.kvs

import akka.actor.ActorSystem

object Run extends App {
  val config = """
    akka {
      loglevel = INFO
      remote {
        netty.tcp {
          hostname = 127.0.0.1
          port = 4281
        }
      }
      cluster {
        seed-nodes = ["akka.tcp://kvs@127.0.0.1:4281"]
      }
    }
  """

  import com.typesafe.config.ConfigFactory
  val cfg = ConfigFactory.parseString(config).withFallback(ConfigFactory.load()).resolve()
  implicit val system = ActorSystem("kvs", cfg)
  val kvs = Kvs(system)

  kvs.onReady{
    val r = kvs.put("a","b")
    system.log.info(s"${r}")
  }

  sys.addShutdownHook {
    import system.dispatcher
    system.terminate()
    import scala.concurrent.Await
    import scala.concurrent.duration.Duration
    Await.result(system.whenTerminated,Duration.Inf)
  }
}
