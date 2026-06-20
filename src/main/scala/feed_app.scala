package feed

import akka.actor.ActorSystem
import akka.cluster.Cluster
import com.typesafe.config.ConfigFactory
import zd.kvs.*
import zd.kvs.idx.IdxHandler
import zio.*
import zio.Console.{printLine, readLine}

final case class Add(feed: String, entryId: String)

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
        consistencyConfig = SeqConsistency.Config(
          name = "index-feeds"
        , handler = {
            case Add(feed, entryId) =>
              ZIO
                .fromEither:
                  val fid = IdxHandler.Fid(feed)
                  kvs.index.add(IdxHandler.Idx(fid, entryId))
                .map(identity[Any])
            case msg => ZIO.fail(InvalidArgument(s"Unsupported feed operation: $msg"))
          }
        , entityId = {
            case Add(feed, _) => feed
            case msg => throw IllegalArgumentException(s"Unsupported feed operation: $msg")
          }
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
