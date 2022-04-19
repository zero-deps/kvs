package kvs.sharding

import akka.actor.{Actor, Props}
import kvs.rng.{AckQuorumFailed, AckTimeoutFailed}
import zio.*

type SeqConsistency = Has[SeqConsistency.Service]

type Err = AckQuorumFailed | AckTimeoutFailed

object SeqConsistency:
  trait Service:
    def send(msg: Any): IO[Err, Any]

  case class Config(name: String, f: Any => IO[Any, Any], id: Any => String)

  val live: ZLayer[ClusterSharding & Has[Config], Nothing, SeqConsistency] =
    ZLayer.fromServicesM[kvs.sharding.Service, Config, Any, Nothing, Service]{ case (sharding, cfg) =>
      for
        shards <-
          sharding.start(
            cfg.name
          , Props(new Actor:
              def receive: Receive =
                a => sender() ! Runtime.default.unsafeRunSync(cfg.f(a))
            )
          , cfg.id)
      yield
        new Service:
          def send(msg: Any): IO[Err, Any] =
            sharding.send(shards, msg)
    }
