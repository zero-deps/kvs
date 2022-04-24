package kvs.feed
package app

import akka.actor.{Actor, Props}
import java.io.IOException
import kvs.rng.{ActorSystem, Dba}
import kvs.sharding.*
import proto.*
import zio.*, stream.*
import zio.Console.{printLine, readLine}

@main
def feedApp: Unit =
  val io: ZIO[Feed & SeqConsistency & Console, Any, Unit] =
    for
      feed <- ZIO.service[Feed]
      seqc <- ZIO.service[SeqConsistency]
      user <- IO.succeed("guest")
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
                          case "" => IO.unit
                          case s => bodyRef.update(_ + "\n" + s)
                    yield s).repeatUntilEquals("")
                  body <- bodyRef.get
                  _ <-
                    body.isEmpty match
                      case true => IO.unit
                      case false =>
                        for
                          post <- IO.succeed(Post(body))
                          answer <- seqc.send(Add(user, post))
                          _ <- printLine(answer.toString)
                        yield ()
                yield ()
              case "all" =>
                all(user).take(5).tap(x => printLine(x._2.body + "\n" + "-" * 10)).runDrain
              case _ => IO.unit
        yield s).repeatUntilEquals("q")
    yield ()

  val akkaConfig: ULayer[ActorSystem.Conf] =
    val name = "app"
    ActorSystem.staticConf(name, kvs.rng.akkaConf(name, "127.0.0.1", 4343) ++ "akka.loglevel=off")
  val dbaConfig: ULayer[kvs.rng.Conf] =
    ZLayer.succeed(kvs.rng.Conf(dir = "target/data"))
  val seqConsistencyConfig: URLayer[Feed, SeqConsistency.Config] =
    ZLayer.fromFunction(feed =>
      SeqConsistency.Config(
        "Posts"
      , {
          case Add(user, post) =>
            (for
              _ <- kvs.feed.add(fid(user), post)
            yield "added").provideService(feed)
        }
      , {
          case Add(user, _) => user
        }
      )
    )
  
  Runtime.default.unsafeRun(io.provide(
    SeqConsistency.live
  , seqConsistencyConfig
  , kvs.sharding.live
  , kvs.feed.live
  , Dba.live
  , dbaConfig
  , ActorSystem.live
  , akkaConfig
  , Console.live
  , Clock.live
  ))

case class Post(@N(1) body: String)

given Codec[Post] = caseCodecAuto

case class Add(user: String, post: Post)

def all(user: String): ZStream[Feed, Err, (Eid, Post)] =
  for
    r <- kvs.feed.all(fid(user))
  yield r

def fid(user: String): String = s"posts.$user"

