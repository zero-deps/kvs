package kvs

import com.typesafe.config.ConfigFactory
import _root_.akka.actor.{ActorSystem => RootActorSystem}
import com.typesafe.config.Config
import zd.proto.Bytes
import zio.{Has, ZIO, ZLayer, RLayer, ULayer}
import kvs.store.{Dba => RootDba, DbaConf => RootDbaConf, RngConf, RksConf, Rng, Rks, MemConf, Mem}

package object seq {
  type Kvs         = KvsList with KvsCircular with KvsFile with KvsSearch
  type KvsList     = Has[KvsList.    Service]
  type KvsCircular = Has[KvsCircular.Service]
  type KvsFile     = Has[KvsFile.    Service]
  type KvsSearch   = Has[KvsSearch.  Service]

  type DbaConf     = Has[RootDbaConf]
  type Dba         = Has[Dba.Service]
  type AkkaConf    = Has[ActorSystem.Conf]
  type ActorSystem = Has[ActorSystem.Service]

  object Dba {
    type Service = RootDba

    def ringConf(conf: Rng.Conf = Rng.Conf()): ULayer[DbaConf] = ZLayer.succeed(RngConf(conf))
    def rksConf(conf: Rks.Conf = Rks.Conf()): ULayer[DbaConf]  = ZLayer.succeed(RksConf(conf))
    def memConf(): ULayer[DbaConf]                             = ZLayer.succeed(MemConf)

    val live: RLayer[ActorSystem with DbaConf, Dba] = ZLayer.fromEffect{
      for {
        actorSystem  <- ZIO.access[ActorSystem](_.get)
        dbaConf      <- ZIO.access[DbaConf](_.get)
        res          <- dbaConf match {
                          case RngConf(conf) => ZIO.effect(Rng(actorSystem, conf): Service)
                          case RksConf(conf) => ZIO.effect(Rks(conf): Service)
                          case MemConf       => ZIO.effect(Mem(): Service)
                          case _             => ZIO.fail(new Exception("not implemented"))
                        }
      } yield res
    }
  }

  object ActorSystem {
    type Service = RootActorSystem
    case class Conf(name: String, config: Config)

    def staticConf(name: String, host: String, port: Int): ULayer[AkkaConf] = {
      val cfg = s"""
        |akka.remote.netty.tcp.hostname = $host
        |akka.remote.netty.tcp.port = $port
        |akka.cluster.seed-nodes = [ "akka.tcp://$name@$host:$port" ]""".stripMargin
      ZLayer.fromEffect(ZIO.succeed(ConfigFactory.parseString(cfg)).map(Conf(name, _)))
    }

    val live: RLayer[AkkaConf, ActorSystem] = ZLayer.fromManaged{
      (for {
        akkaConf <- ZIO.access[AkkaConf](_.get)
        res      <- ZIO.effect(RootActorSystem(akkaConf.name, akkaConf.config))
      } yield res)
        .toManaged(as => ZIO.fromFuture(_ => as.terminate()).either)
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
