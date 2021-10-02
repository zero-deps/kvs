package kvs

import _root_.akka.actor.{ActorSystem as RootActorSystem}
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import kvs.store.{Dba as RootDba, DbaConf as RootDbaConf, RngConf, Rng}
import zio.{Has, ZIO, ZLayer, RLayer, ULayer}
import zio.clock.Clock
import zio.stream.ZStream

package object seq {
  type Kvs = KvsFeed
  type KvsFeed = Has[KvsFeed.Service]

  type DbaConf = Has[RootDbaConf]
  type Dba = Has[Dba.Service]
  type AkkaConf = Has[ActorSystem.Conf]
  type ActorSystem = Has[ActorSystem.Service]

  object Dba {
    type Service = RootDba

    def rngConf(conf:Rng.Conf=Rng.Conf()): ULayer[DbaConf] = ZLayer.succeed(RngConf(conf))

    val live: RLayer[ActorSystem with DbaConf with Clock, Dba] = ZLayer.fromEffect{
      for
        actorSystem  <- ZIO.service[ActorSystem.Service]
        dbaConf <- ZIO.service[RootDbaConf]
        clock <- ZIO.service[Clock.Service]
        res <-
          dbaConf match
            case RngConf(conf) => ZIO.effect(Rng(actorSystem, conf, clock): Service)
      yield res
    }
  }

  object ActorSystem {
    type Service = RootActorSystem
    case class Conf(name: String, config: Config)

    def staticConf(name: String, host: String, port: Int, ext: String=""): ULayer[AkkaConf] = {
      val cfg = s"""
        akka {
          actor {
            provider = cluster
            deployment {
              /ring_readonly_store {
                router = round-robin-pool
                nr-of-instances = 5
              }
            }
            debug {
              receive   = off
              lifecycle = off
            }
            serializers {
              kvsproto = kvs.Serializer
            }
            serialization-identifiers {
              "kvs.Serializer" = 50
            }
            serialization-bindings {
              "kvs.rng.model.ChangeState"         = kvsproto
              "kvs.rng.model.StoreGetAck"         = kvsproto
              "kvs.rng.model.StoreDelete"         = kvsproto
              "kvs.rng.model.StoreGet"            = kvsproto
              "kvs.rng.model.StorePut"            = kvsproto
              "kvs.rng.model.DumpBucketData"      = kvsproto
              "kvs.rng.model.DumpGetBucketData"   = kvsproto
              "kvs.rng.model.ReplBucketPut"       = kvsproto
              "kvs.rng.model.ReplBucketUpToDate"  = kvsproto
              "kvs.rng.model.ReplGetBucketIfNew"  = kvsproto
              "kvs.rng.model.ReplNewerBucketData" = kvsproto
            }
          }
          remote.artery.canonical {
            hostname = $host
            port = $port
          }
          cluster.seed-nodes = [ "akka://$name@$host:$port" ]
        }
        $ext
        """
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
  def hex(bytes: Array[Byte]): String = {
    val hexChars = new Array[Byte](bytes.length * 2)
    var i = 0
    while (i < bytes.length) {
        val v = bytes(i) & 0xff
        hexChars(i * 2) = hexs(v >>> 4)
        hexChars(i * 2 + 1) = hexs(v & 0x0f)
        i = i + 1
    }
    new String(hexChars, "utf8")
  }
}
