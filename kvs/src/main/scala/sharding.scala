package zd.kvs

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.cluster.sharding.{ClusterSharding as AkkaClusterSharding, ClusterShardingSettings, ShardRegion}
import zio.*

/** ZIO facade over Akka Cluster Sharding. */
trait ClusterSharding:
  def start(name: String, props: Props, entityId: Any => String): UIO[ActorRef]
  def send[A, E](shardRegion: ActorRef, msg: Any): IO[E, A]
end ClusterSharding

object ClusterSharding:
  val layer: URLayer[ActorSystem, ClusterSharding] =
    ZLayer:
      for
        system <- ZIO.service[ActorSystem]
        sharding <- ZIO.succeed(AkkaClusterSharding(system))
      yield
        new ClusterSharding:
          def start(name: String, props: Props, entityId: Any => String): UIO[ActorRef] =
            ZIO.succeed:
              sharding.start(
                typeName = name
              , entityProps = props
              , settings = ClusterShardingSettings(system)
              , extractEntityId = {
                  case msg => entityId(msg) -> msg
                }: ShardRegion.ExtractEntityId
              , extractShardId = {
                  case msg => Math.floorMod(entityId(msg).hashCode, 100).toString
                }: ShardRegion.ExtractShardId
              )

          def send[A, E](shardRegion: ActorRef, msg: Any): IO[E, A] =
            ZIO.async: callback =>
              val receiver = system.actorOf(Props(new Receiver[A, E](callback)))
              shardRegion.tell(msg, receiver)
end ClusterSharding

private final class Receiver[A, E](handler: IO[E, A] => Unit) extends Actor:
  def receive: Receive =
    case Exit.Success(value) =>
      handler(ZIO.succeed(value.asInstanceOf[A]))
      context.stop(self)
    case Exit.Failure(cause) =>
      handler(ZIO.failCause(cause.asInstanceOf[Cause[E]]))
      context.stop(self)
    case value =>
      handler(ZIO.dieMessage(s"Unexpected sharding response: $value"))
      context.stop(self)
end Receiver
