package kvs.sharding

import akka.actor.{Actor, Props}
import kvs.rng.DbaErr
import zio.*

trait SeqConsistency:
  def send(msg: Any): IO[DbaErr, Any]

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
                a => sender() ! Unsafe.unsafe(Runtime.default.unsafe.run(cfg.f(a)))
            )
          , cfg.id)
      yield
        new SeqConsistency:
          def send(msg: Any): IO[DbaErr, Any] =
            sharding.send(shards, msg)
    )
