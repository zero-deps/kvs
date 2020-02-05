package zd.kvs

import scala.util.Try
import zd.proto.Bytes
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
    val k1 = Bytes.unsafeWrap(Array[Byte](1))
    val v1 = Bytes.unsafeWrap("hi".getBytes)
    kvs.el.put(k1, v1)
    println(kvs.el.get(k1).map(_.map(_.unsafeArray).map(xs => new String(xs))))
  }
}
