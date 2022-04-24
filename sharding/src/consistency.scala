package kvs.sharding

import akka.actor.{Actor, Props}
import kvs.rng.{AckQuorumFailed, AckTimeoutFailed}
import zio.*

trait SeqConsistency:
  def send(msg: Any): IO[Err, Any]

type Err = AckQuorumFailed | AckTimeoutFailed

object SeqConsistency:
  case class Config(name: String, f: Any => IO[Any, Any], id: Any => String)

  val live: ZLayer[ClusterSharding & Config, Nothing, SeqConsistency] =
    ZLayer(
      for
        sharding <- ZIO.service[ClusterSharding]
        cfg <- ZIO.service[Config]
        shards <-
          sharding.start(
            cfg.name
          , Props(new Actor:
              def receive: Receive =
                a => sender() ! Runtime.default.unsafeRunSync(cfg.f(a))
            )
          , cfg.id)
      yield
        new SeqConsistency:
          def send(msg: Any): IO[Err, Any] =
            sharding.send(shards, msg)
    )
