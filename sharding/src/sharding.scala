package kvs.sharding

import akka.actor.{Actor, ActorRef, Props}
import akka.cluster.sharding.{ClusterSharding as AkkaClusterSharding, ClusterShardingSettings, ShardRegion}
import kvs.rng.ActorSystem
import zio.*

type ClusterSharding = Has[Service]

trait Service:
  def start[A](name: String, props: Props, id: A => String): UIO[ActorRef]
  def send[A, E](shardRegion: ActorRef, msg: Any): IO[E, A]

val live: URLayer[ActorSystem, ClusterSharding] =
  ZLayer.fromEffect(
    for
      system <- ZIO.service[ActorSystem.Service]
      sharding <- IO.effectTotal(AkkaClusterSharding(system))
    yield
      new Service:
        def start[A](name: String, props: Props, id: A => String): UIO[ActorRef] =
          ZIO.effectTotal(
            sharding.start(
              typeName = name,
              entityProps = props,
              settings = ClusterShardingSettings(system),
              extractEntityId = {
                case msg: A => (id(msg), msg)
              }: ShardRegion.ExtractEntityId,
              extractShardId = {
                case msg: A => (math.abs(id(msg).hashCode) % 100).toString
              }: ShardRegion.ExtractShardId,
            )
          )
        
        def send[A, E](shardRegion: ActorRef, msg: Any): IO[E, A] =
          ZIO.effectAsyncM{ (callback: IO[E, A] => Unit) =>
            for
              receiver <- IO.effectTotal(system.actorOf(Props(Receiver[A, E]{
                case Exit.Success(a) => callback(IO.succeed(a))
                case Exit.Failure(e) => callback(IO.halt(e))
              })))
              _ <- IO.effectTotal(shardRegion.tell(msg, receiver))
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
