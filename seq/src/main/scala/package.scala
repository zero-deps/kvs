package kvs

import com.typesafe.config.ConfigFactory
import _root_.akka.actor.{ActorSystem => RootActorSystem}
import com.typesafe.config.Config
import zd.proto.Bytes
import zio.{Has, ZIO, Task, TaskLayer, ZLayer, RLayer, ULayer}
import kvs.store.{Dba => RootDba, DbaConf => RootDbaConf, RngConf, Rng, MemConf, Mem}

package object seq {
  type Kvs       = KvsList with KvsArray with KvsFile with KvsSearch
  type KvsList   = Has[KvsList.Service]
  type KvsArray  = Has[KvsArray.Service]
  type KvsFile   = Has[KvsFile.Service]
  type KvsSearch = Has[KvsSearch.Service]
  
  type DbaConf     = Has[RootDbaConf]
  type Dba         = Has[Dba.Service]
  type ActorSystem = Has[ActorSystem.Service]

  object Dba {
    type Service = RootDba

    def ringConf(conf: Rng.Conf = Rng.Conf()): ULayer[DbaConf] = ZLayer.succeed(RngConf(conf))

    val live: RLayer[ActorSystem with DbaConf, Dba] = ZLayer.fromEffect{
      for {
        actorSystem  <- ZIO.access[ActorSystem](_.get)
        dbaConf      <- ZIO.access[DbaConf](_.get)
        res          <- dbaConf match {
                          case RngConf(conf) => Task.effect(Rng(actorSystem, conf): Service)
                          case MemConf       => Task.effect(Mem(): Service)
                          case _             => Task.fail(new Exception("not implemented"))
                        }
      } yield res
    }
  }

  object ActorSystem {
    type Service = RootActorSystem

    def live(name: String, host: String, port: Int): TaskLayer[ActorSystem] = ZLayer.fromManaged{
      val cfg = s"""
      |akka.remote.netty.tcp.hostname = $host
      |akka.remote.netty.tcp.port = $port
      |akka.cluster.seed-nodes = [ "akka.tcp://$name@$host:$port" ]""".stripMargin
      val akkaConf = ConfigFactory.parseString(cfg)
      ZIO.effect(RootActorSystem(name)).toManaged(as => Task.fromFuture(_ => as.terminate()).either)
    }

    def live(name: String, config: Config): TaskLayer[ActorSystem] = ZLayer.fromManaged{
      ZIO.effect(RootActorSystem(name, config)).toManaged(as => Task.fromFuture(_ => as.terminate()).either)
    }
  }

  val hexs = "0123456789abcdef".getBytes("ascii")
  def hex(bytes: Bytes): String = {
    val hexChars = new Array[Byte](bytes.length * 2)
    var i = 0
    while (i < bytes.length) {
        val v = bytes.unsafeArray(i) & 0xff
        hexChars(i * 2) = hexs(v >>> 4)
        hexChars(i * 2 + 1) = hexs(v & 0x0f)
        i = i + 1
    }
    new String(hexChars, "utf8")
  }
}
