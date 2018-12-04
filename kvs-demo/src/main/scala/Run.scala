package mws.kvs

import scala.util.Try

import akka.actor.ActorSystem

object Run extends App {
  // val config = """
  //   akka {
  //     loglevel = INFO
  //     remote {
  //       netty.tcp {
  //         hostname = 127.0.0.1
  //         port = 4281
  //       }
  //     }
  //     cluster {
  //       seed-nodes = ["akka.tcp://kvs@127.0.0.1:4281"]
  //     }
  //   }
  // """

  import com.typesafe.config.ConfigFactory
  val cfg = ConfigFactory.load()
  implicit val system = ActorSystem("kvs", cfg)
  val kvs = Kvs(system)

  sys.addShutdownHook {
    system.terminate()
    import scala.concurrent.Await
    import scala.concurrent.duration.Duration
    Try(Await.result(system.whenTerminated,Duration.Inf))
  }

<<<<<<< HEAD
  import system.dispatcher

  kvs.onReady.map{ _ =>
    val r = kvs.el.put("a","b")
    system.log.info(s"${r}")
    system.log.info(kvs.nextid("fid").toString)
=======
  kvs.onReady{
    kvs.load("/home/anle/perf_data/rng_dump_2018.11.21-18.20.48.zip")
>>>>>>> dbf6ee8... protobuff
  }
}

object Stat {
  def get(kvs: Kvs): Unit = {
    val info: java.util.Map[Key, Info] = new java.util.HashMap(100000)

    def update(k: Key, size: Long): Unit = {
      val stat = Option(info.get(k)).map(i => Info(i.count+1, i.size+size)).getOrElse(Info(1, size))
      info.put(k, stat)
    }

    kvs.iterate("/home/anle/perf_data/rng_dump_2018.11.21-18.20.48.zip", (key, data) => {
      val parts = key.split("_")
      parts match {
        case Array("translations", "b", brandId, "commit", _*) =>
          update(Key("translations", "commit", "0"), data.size)
        case Array("translations", "history", "b", brandId, _*) =>
          update(Key("translations", "history", "0"), data.size)
        case Array("translations", "b", brandId, "k", _*) =>
          update(Key("translations", "keys", "0"), data.size)
        case Array("translations", "b", brandId, _*) =>
          update(Key("translations", "main", "0"), data.size)

        case Array("translations", "s", siteId, "commit", _*) =>
          update(Key("translations", "commit", siteId), data.size)
        case Array("translations", "history", "s", siteId, _*) =>
          update(Key("translations", "history", siteId), data.size)
        case Array("translations", "s", siteId, "k", _*) =>
          update(Key("translations", "keys", siteId), data.size)
        case Array("translations", "s", siteId, _*) =>
          update(Key("translations", "main", siteId), data.size)

        case Array("properties", "history", siteId, _*) => 
          update(Key("properties", "history", siteId), data.size)
        case Array("properties", siteId, "commit", _*) => 
          update(Key("properties", "commit", siteId), data.size)
        case Array("properties", siteId, _*) => 
          update(Key("properties", "main", siteId), data.size)

        case Array("structures", "history", siteId, _*) => 
          update(Key("structures", "history", siteId), data.size)
        case Array("structures", siteId, "commit", _*) => 
          update(Key("structures", "commit", siteId), data.size)
        case Array("structures", "names", siteId, _*) => 
          update(Key("structures", "names", siteId), data.size)
        case Array("structures", siteId, _*) => 
          update(Key("structures", "main", siteId), data.size)
        
        case Array("templates", "history", siteId, _*) => 
          update(Key("templates", "history", siteId), data.size)
        case Array("templates", siteId, "commit", _*) => 
          update(Key("templates", "commit", siteId), data.size)
        case Array("templates", "names", siteId, _*) => 
          update(Key("templates", "names", siteId), data.size)
        case Array("templates", siteId, _*) => 
          update(Key("templates", "main", siteId), data.size)

        case Array("webcontents", "history", siteId, _*) => 
          update(Key("webcontents", "history", siteId), data.size)
        case Array("webcontents", siteId, "commit", _*) => 
          update(Key("webcontents", "commit", siteId), data.size)
        case Array("webcontents", "names", siteId, _*) => 
          update(Key("webcontents", "names", siteId), data.size)
        case Array("webcontents", "rfr", siteId, _*) =>
          update(Key("webcontents", "rfr", siteId), data.size)
        case Array("webcontents", "rejected", siteId, _*) =>
          update(Key("webcontents", "rejected", siteId), data.size)
        case Array("webcontents", "production", siteId, _*) =>
          update(Key("webcontents", "production", siteId), data.size)
        case Array("webcontents", siteId, _*) => 
          update(Key("webcontents", "main", siteId), data.size)

        case unknown =>
          println("unknown key=" + key)
      }
    })
    import scala.collection.JavaConverters._
    println(s"stat")
    info.asScala.toList.map{ case (key, info) =>
      println(s"${key.component},${key.feed},${key.site},${info.count},${info.size}")
    }
  }
}

case class Key(component: String, feed: String, site: String)
case class Info(count: Long, size: Long)
