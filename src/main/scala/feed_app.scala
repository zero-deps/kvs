package feed

import akka.actor.ActorSystem
import akka.cluster.Cluster
import com.typesafe.config.ConfigFactory
import proto.*
import zd.kvs.*
import zd.kvs.idx.IdxHandler
import zd.kvs.idx.IdxHandler.given
import zio.*
import zio.Console.{printLine, readLine}

final case class Add(@N(1) feed: String, @N(2) entryId: String)
given MessageCodec[Add] = caseCodecAuto

/** Example of serializing updates independently for each index feed. */
object FeedApp extends ZIOAppDefault:
  def run: ZIO[Any, Any, Unit] =
    ZIO.scoped:
      for
        system <- ZIO.acquireRelease(
          ZIO.attempt:
            val config = ConfigFactory
              .parseString("akka.remote.artery.canonical.port=0")
              .withFallback(ConfigFactory.load())
            ActorSystem("feed-app", config)
        )(system => ZIO.fromFuture(_ => system.terminate()).orDie)
        _ <- ZIO.succeed(Cluster(system).join(Cluster(system).selfAddress))
        kvs <- ZIO.acquireRelease(ZIO.succeed(Kvs.mem()))(kvs => ZIO.succeed(kvs.close()))
        consistencyConfig = SeqConsistency.Config[Add, IdxHandler.Idx](
          name = "index-feeds"
        , handler = add =>
            ZIO.fromEither:
              val fid = IdxHandler.Fid(add.feed)
              kvs.index.add(IdxHandler.Idx(fid, add.entryId))
        , entityId = _.feed
        )
        program =
          for
            consistency <- ZIO.service[SeqConsistency]
            _ <- printLine("Commands: add <feed> <entry-id>, all <feed>, q")
            _ <- (
              for
                line <- readLine
                _ <- line.trim.split("\\s+").toList match
                  case "add" :: feed :: entryId :: Nil =>
                    consistency
                      .send[IdxHandler.Idx](Add(feed, entryId))
                      .flatMap(entry => printLine(s"added ${entry.id} to $feed"))
                  case "all" :: feed :: Nil =>
                    ZIO
                      .fromEither(kvs.index.all(IdxHandler.Fid(feed)))
                      .flatMap(entries => printLine(entries.flatMap(_.toOption).map(_.id).mkString(", ")))
                  case "q" :: Nil => ZIO.unit
                  case _ => printLine("Unknown command")
              yield line
            ).repeatUntil(_.trim == "q")
          yield ()
        _ <- program.provide(
          SeqConsistency.layer
        , ClusterSharding.layer
        , ZLayer.succeed(consistencyConfig)
        , ZLayer.succeed(system)
        )
      yield ()
end FeedApp
