package kvs.sort
package app

import akka.actor.{Actor, Props}
import java.io.IOException
import kvs.rng.{ActorSystem, Dba}
import kvs.sharding.*
import proto.*
import zio.*, stream.*
import zio.Console.{printLine, readLine}

@main
def sortApp: Unit =
  val io: ZIO[Sort & SeqConsistency & Console, Any, Unit] =
    for
      sort <- ZIO.service[Sort]
      seqc <- ZIO.service[SeqConsistency]
      _ <- printLine("welcome!")
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
                          answer <- seqc.send(Add(post))
                          _ <- printLine(answer.toString)
                        yield ()
                yield ()
              case "all" =>
                all().take(5).tap(x => printLine(x.body + "\n" + "-" * 10)).runDrain
              case _ => IO.unit
        yield s).repeatUntilEquals("q")
    yield ()

  val akkaConfig: ULayer[ActorSystem.Conf] =
    val name = "app"
    ActorSystem.staticConf(name, kvs.rng.akkaConf(name, "127.0.0.1", 4343) ++ "akka.loglevel=off")
  val dbaConfig: ULayer[kvs.rng.Conf] =
    ZLayer.succeed(kvs.rng.Conf(dir = "target/data"))
  val seqConsistencyConfig: URLayer[Sort, SeqConsistency.Config] =
    ZLayer.fromFunction(sort =>
      SeqConsistency.Config(
        "Posts"
      , {
          case Add(post) =>
            (for
              _ <- kvs.sort.insert(ns, post)
            yield "added").provideService(sort)
        }
      , _ => "shard"
      )
    )
  
  Runtime.default.unsafeRun(io.provide(
    SeqConsistency.live
  , seqConsistencyConfig
  , kvs.sharding.live
  , SortLive.layer
  , Dba.live
  , dbaConfig
  , ActorSystem.live
  , akkaConfig
  , Console.live
  , Clock.live
  ))

case class Post(@N(1) body: String)

given Codec[Post] = caseCodecAuto
given Ordering[Post] = Ordering.by(_.body)
given CanEqual[Post, Post] = CanEqual.derived

case class Add(post: Post)

def all(): ZStream[Sort, Err, Post] =
  for
    r <- kvs.sort.flatten(ns)
  yield r

val ns = "posts_sorted"

