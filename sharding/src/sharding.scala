package kvs.sharding

import org.apache.pekko.actor.{Actor, ActorRef, Props}
import org.apache.pekko.cluster.sharding.{ClusterSharding as PekkoClusterSharding, ClusterShardingSettings, ShardRegion}
import kvs.rng.ActorSystem
import zio.*

trait ClusterSharding:
  def start[A](name: String, props: Props, id: A => String): UIO[ActorRef]
  def send[A, E](shardRegion: ActorRef, msg: Any): IO[E, A]

val live: URLayer[ActorSystem, ClusterSharding] =
  ZLayer(
    for
      system <- ZIO.service[ActorSystem]
      sharding <- ZIO.succeed(PekkoClusterSharding(system))
    yield
      new ClusterSharding:
        def start[A](name: String, props: Props, id: A => String): UIO[ActorRef] =
          ZIO.succeed(
            sharding.start(
              typeName = name,
              entityProps = props,
              settings = ClusterShardingSettings(system),
              extractEntityId = {
                case msg: A => (id(msg), msg)
              }: ShardRegion.ExtractEntityId,
              extractShardId = {
                case msg => (math.abs(id(msg.asInstanceOf[A]).hashCode) % 100).toString
              }: ShardRegion.ExtractShardId,
            )
          )
        
        def send[A, E](shardRegion: ActorRef, msg: Any): IO[E, A] =
          ZIO.asyncZIO{ (callback: IO[E, A] => Unit) =>
            for
              receiver <- ZIO.succeed(system.actorOf(Props(Receiver[A, E]{
                case Exit.Success(a) => callback(ZIO.succeed(a))
                case Exit.Failure(e) => callback(ZIO.failCause(e))
              })))
              _ <- ZIO.succeed(shardRegion.tell(msg, receiver))
            yield ()
          }
  )

class Receiver[A, E](handler: Exit[E, A] => Unit) extends Actor:
  def receive: Receive =
    case r: Exit[E, A] =>
      handler(r)
      context.stop(self)
    case x =>
      println(x.toString)
      context.stop(self)
