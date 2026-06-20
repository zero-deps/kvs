package zd.kvs

import akka.actor.{Actor, Props}
import zio.*

/** Serializes operations sharing the same entity ID through one shard entity. */
trait SeqConsistency:
  def send[A](msg: Any): IO[Err, A]
end SeqConsistency

object SeqConsistency:
  final case class Config(
    name: String
  , handler: Any => IO[Err, Any]
  , entityId: Any => String
  )

  val layer: RLayer[ClusterSharding & Config, SeqConsistency] =
    ZLayer:
      for
        sharding <- ZIO.service[ClusterSharding]
        config <- ZIO.service[Config]
        shards <- sharding.start(
          config.name
        , Props(new Actor:
            def receive: Receive =
              case msg =>
                val replyTo = sender()
                val result = Unsafe.unsafely(Runtime.default.unsafe.run(config.handler(msg)))
                replyTo ! result
          )
        , config.entityId
        )
      yield
        new SeqConsistency:
          def send[A](msg: Any): IO[Err, A] =
            sharding.send[A, Err](shards, msg)
end SeqConsistency
