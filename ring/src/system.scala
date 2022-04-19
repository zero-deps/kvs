package kvs.rng

import akka.actor.{ActorSystem as RootActorSystem}
import com.typesafe.config.{ConfigFactory, Config}
import zio.*

type ActorSystem = Has[ActorSystem.Service]

object ActorSystem:
  type Service = RootActorSystem

  case class Conf(name: String, config: Config)

  def staticConf(name: String, cfg: String): ULayer[Has[Conf]] =
    ZLayer.fromEffect(ZIO.succeed(ConfigFactory.parseString(cfg).nn).map(Conf(name, _)))

  val live: RLayer[Has[Conf], ActorSystem] =
    ZLayer.fromManaged(
      (for
        conf <- ZIO.service[Conf]
        system <- ZIO.effect(RootActorSystem(conf.name, conf.config))
      yield system)
        .toManaged(system => ZIO.fromFuture(_ => system.terminate()).either)
    )
