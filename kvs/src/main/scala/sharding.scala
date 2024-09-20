package kvs

import org.apache.pekko.actor.{Actor, ActorRef, Props}
import org.apache.pekko.cluster.sharding.{ClusterSharding as PekkoClusterSharding, ClusterShardingSettings, ShardRegion}
import zd.rng.ActorSystem
import zio.*

trait ClusterSharding:
  def start[A](name: String, props: Props, id: A => String): UIO[ActorRef]
  def send[A, E](shardRegion: ActorRef, msg: Any): IO[E, A]
end ClusterSharding

object ClusterSharding:
  val layer: URLayer[ActorSystem, ClusterSharding] =
    ZLayer(
      for
        system <- ZIO.service[ActorSystem]
        sharding <- ZIO.succeed(PekkoClusterSharding(system))
      yield
        new ClusterSharding:
          def start[A](name: String, props: Props, id: A => String): UIO[ActorRef] =
            ZIO.succeed:
              sharding.start(
                typeName = name
              , entityProps = props
              , settings = ClusterShardingSettings(system)
              , extractEntityId = {
                  case msg: A @unchecked => (id(msg), msg)
                }: ShardRegion.ExtractEntityId
              , extractShardId = {
                  case msg => (math.abs(id(msg.asInstanceOf[A]).hashCode) % 100).toString
                }: ShardRegion.ExtractShardId
              )
          
          def send[A, E](shardRegion: ActorRef, msg: Any): IO[E, A] =
            ZIO.asyncZIO:
              (callback: IO[E, A] => Unit) =>
                ZIO.succeed:
                  shardRegion.tell(
                    msg
                  , system.actorOf:
                      Props:
                        Receiver[A, E]:
                          case Exit.Success(a) => callback(ZIO.succeed(a))
                          case Exit.Failure(e) => callback(ZIO.failCause(e))
                  )
    )
end ClusterSharding

class Receiver[A, E](handler: Exit[E, A] => Unit) extends Actor:
  def receive: Receive =
    case r: Exit[E @unchecked, A @unchecked] =>
      handler(r)
      context.stop(self)
    case x =>
      println(x.toString)
      context.stop(self)
end Receiver
