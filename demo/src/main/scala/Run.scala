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
    // log.warning(kvs.el.put("k", "v").toString)
    // log.warning(kvs.el.get[String]("k").toString)
    // log.warning(kvs.el.delete[String]("k").toString)

    // log.warning(kvs.nextid("fid").toString)

    // Thread.sleep(240000)
    // kvs.dump.load("/home/anle/perf_data/rng_dump_2018.12.10-16.00.57")

    // system.log.info("start!!!")
    // (1 to 1000).map { i =>
    // (1 to 100000).map { i =>
    //   kvs.el.put(i.toString, i.toString)
    // }
    // system.log.info("written")
  }
}
