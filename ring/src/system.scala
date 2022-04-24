package kvs.rng

import com.typesafe.config.{ConfigFactory, Config}
import zio.*

type ActorSystem = akka.actor.ActorSystem

object ActorSystem:
  case class Conf(name: String, config: Config)

  def staticConf(name: String, cfg: String): ULayer[Conf] =
    ZLayer(ZIO.succeed(ConfigFactory.parseString(cfg).nn).map(Conf(name, _)))

  val live: RLayer[Conf, ActorSystem] =
    ZLayer.scoped(
      ZIO.acquireRelease(
        for
          conf <- ZIO.service[Conf]
          system <- ZIO.attempt(akka.actor.ActorSystem(conf.name, conf.config))
        yield system
      )(
        system => ZIO.fromFuture(_ => system.terminate()).either
      )
    )
