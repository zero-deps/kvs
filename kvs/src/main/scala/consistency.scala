package kvs

import org.apache.pekko.actor.{Actor, Props}
import zio.*

trait SeqConsistency:
  def send(msg: Any): IO[DbaErr, Any]
end SeqConsistency

object SeqConsistency:
  case class Config(name: String, f: Any => IO[Any, Any], id: Any => String)

  val layer: RLayer[ClusterSharding & Config, SeqConsistency] =
    ZLayer(
      for
        sharding <- ZIO.service[ClusterSharding]
        cfg <- ZIO.service[Config]
        shards <-
          sharding.start(
            cfg.name
          , Props(new Actor:
              def receive: Receive =
                a => sender() ! Unsafe.unsafely(Runtime.default.unsafe.run(cfg.f(a)))
            )
          , cfg.id)
      yield
        new SeqConsistency:
          def send(msg: Any): IO[DbaErr, Any] =
            sharding.send(shards, msg)
    )
end SeqConsistency
