package zd.kvs

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.cluster.sharding.{ClusterSharding as AkkaClusterSharding, ClusterShardingSettings, ShardRegion}
import proto.*
import zio.*

sealed trait ShardingMessage

@N(1) final case class ShardRequest(
  @N(1) entityId: String
, @N(2) payload: Array[Byte]
) extends ShardingMessage

@N(2) final case class ShardSuccess(@N(1) payload: Array[Byte]) extends ShardingMessage
@N(3) final case class ShardFailure(@N(1) error: WireError) extends ShardingMessage
@N(4) final case class ShardDefect(@N(1) message: String) extends ShardingMessage

final case class WireError(
  @N(1) code: String
, @N(2) values: List[String]
)

private given MessageCodec[ShardRequest] = caseCodecAuto
private given MessageCodec[ShardSuccess] = caseCodecAuto
private given MessageCodec[WireError] = caseCodecAuto
private given MessageCodec[ShardFailure] = caseCodecAuto
private given MessageCodec[ShardDefect] = caseCodecAuto
private[kvs] given shardingMessageCodec: MessageCodec[ShardingMessage] = sealedTraitCodecAuto

/** ZIO facade over Akka Cluster Sharding. */
trait ClusterSharding:
  def start(name: String, props: Props): UIO[ActorRef]
  private[kvs] def send(shardRegion: ActorRef, msg: ShardRequest): UIO[ShardingMessage]
end ClusterSharding

object ClusterSharding:
  val layer: URLayer[ActorSystem, ClusterSharding] =
    ZLayer:
      for
        system <- ZIO.service[ActorSystem]
        sharding <- ZIO.succeed(AkkaClusterSharding(system))
      yield
        new ClusterSharding:
          def start(name: String, props: Props): UIO[ActorRef] =
            ZIO.succeed:
              sharding.start(
                typeName = name
              , entityProps = props
              , settings = ClusterShardingSettings(system)
              , extractEntityId = {
                  case msg: ShardRequest => msg.entityId -> msg
                }: ShardRegion.ExtractEntityId
              , extractShardId = {
                  case msg: ShardRequest => Math.floorMod(msg.entityId.hashCode, 100).toString
                }: ShardRegion.ExtractShardId
              )

          private[kvs] def send(shardRegion: ActorRef, msg: ShardRequest): UIO[ShardingMessage] =
            ZIO.asyncInterrupt: callback =>
              val receiver = system.actorOf(Props(new Receiver(callback)))
              shardRegion.tell(msg, receiver)
              Left(ZIO.succeed(system.stop(receiver)))
end ClusterSharding

private final class Receiver(handler: UIO[ShardingMessage] => Unit) extends Actor:
  def receive: Receive =
    case value: ShardingMessage =>
      handler(ZIO.succeed(value))
      context.stop(self)
    case value =>
      handler(ZIO.dieMessage(s"Unexpected sharding response: $value"))
      context.stop(self)
end Receiver
