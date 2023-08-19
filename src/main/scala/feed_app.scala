package kvs.feed
package app

import org.apache.pekko.actor.{Actor, Props}
import java.io.IOException
import kvs.rng.{ActorSystem, Dba}
import kvs.sharding.*
import proto.*
import zio.*, stream.*
import zio.Console.{printLine, readLine}

object FeedApp extends ZIOAppDefault:
  def run =
    val io: ZIO[Feed & SeqConsistency, Any, Unit] =
      for
        feed <- ZIO.service[Feed]
        seqc <- ZIO.service[SeqConsistency]
        user <- ZIO.succeed("guest")
        _ <- printLine(s"welcome, $user")
        _ <-
          (for
            _ <- printLine("add/all/q?")
            s <- readLine 
            _ <-
              s match
                case "add" =>
                  for
                    bodyRef <- Ref.make("")
                    _ <- printLine("enter post")
                    _ <-
                      (for
                        s <- readLine
                        _ <-
                          s match
                            case "" => ZIO.unit
                            case s => bodyRef.update(_ + "\n" + s)
                      yield s).repeatUntilEquals("")
                    body <- bodyRef.get
                    _ <-
                      body.isEmpty match
                        case true => ZIO.unit
                        case false =>
                          for
                            post <- ZIO.succeed(Post(body))
                            answer <- seqc.send(Add(user, post))
                            _ <- printLine(answer.toString)
                          yield ()
                  yield ()
                case "all" =>
                  all(user).take(5).tap(x => printLine(x._2.body + "\n" + "-" * 10)).runDrain
                case _ => ZIO.unit
          yield s).repeatUntilEquals("q")
      yield ()

    val pekkoConfig: ULayer[ActorSystem.Conf] =
      val name = "app"
      ActorSystem.staticConf(name, kvs.rng.pekkoConf(name, "127.0.0.1", 4343) ++ "pekko.loglevel=off")
    val dbaConfig: ULayer[kvs.rng.Conf] =
      ZLayer.succeed(kvs.rng.Conf(dir = "target/data"))
    val seqConsistencyConfig: URLayer[Feed, SeqConsistency.Config] =
      ZLayer.fromFunction((feed: Feed) =>
        SeqConsistency.Config(
          "Posts"
        , {
            case Add(user, post) =>
              (for
                _ <- kvs.feed.add(fid(user), post)
              yield "added").provideLayer(ZLayer.succeed(feed))
          }
        , {
            case Add(user, _) => user
          }
        )
      )
    
    io.provide(
      SeqConsistency.live
    , seqConsistencyConfig
    , kvs.sharding.live
    , kvs.feed.live
    , Dba.live
    , dbaConfig
    , ActorSystem.live
    , pekkoConfig
    )
end FeedApp

case class Post(@N(1) body: String)

given Codec[Post] = caseCodecAuto

case class Add(user: String, post: Post)

def all(user: String): ZStream[Feed, Err, (Eid, Post)] =
  for
    r <- kvs.feed.all(fid(user))
  yield r

def fid(user: String): String = s"posts.$user"

