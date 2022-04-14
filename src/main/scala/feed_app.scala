package kvs.feed
package app

import akka.actor.{Actor, Props}
import java.io.IOException
import kvs.rng.{ActorSystem, Dba}
import kvs.sharding.*
import proto.*
import zio.*, stream.*, clock.*, console.*

@main
def feedApp: Unit =
  val io: ZIO[Feed & ClusterSharding & Console, Any, Unit] =
    for
      feed <- ZIO.service[Feed.Service]
      sharding <- ZIO.service[ClusterSharding.Service]
      shards <-
        sharding.start("Posts", Props(Posts(feed)), {
          case Posts.Add(user, _) => user
        })
      user <- IO.succeed("guest")
      _ <- putStrLn(s"welcome, $user")
      _ <-
        (for
          _ <- putStrLn("add/all/quit?")
          s <- getStrLn 
          _ <-
            s match
              case "add" =>
                for
                  bodyRef <- Ref.make("")
                  _ <- putStrLn("enter post")
                  _ <-
                    (for
                      s <- getStrLn
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
                          post <- IO.succeed(Posts.Post(body))
                          answer <- sharding.send[String, Err](shards, Posts.Add(user, post))
                          _ <- putStrLn(answer.toString)
                        yield ()
                yield ()
              case "all" =>
                Posts.all(user).take(5).tap(x => putStrLn(x._2.body + "\n" + "-" * 10)).runDrain
              case _ => IO.unit
        yield s).repeatUntilEquals("quit")
    yield ()

  val name = "app"
  val akkaConf: ULayer[Has[ActorSystem.Conf]] =
    ActorSystem.staticConf(name, kvs.rng.akkaConf(name, "127.0.0.1", 4343) ++ "akka.loglevel=off")
  val actorSystem: TaskLayer[ActorSystem] =
    akkaConf >>> ActorSystem.live
  val dbaConf: ULayer[Has[kvs.rng.Conf]] =
    ZLayer.fromEffect(ZIO.succeed(kvs.rng.Conf(dir = "target/data")))
  val dba: TaskLayer[Dba] =
    actorSystem ++ dbaConf ++ Clock.live >>> Dba.live
  val feedLayer: TaskLayer[Feed] =
    actorSystem ++ dba >>> Feed.live
  val shardingLayer: TaskLayer[ClusterSharding] =
    actorSystem >>> ClusterSharding.live
  
  Runtime.default.unsafeRun(io.provideCustomLayer(feedLayer ++ shardingLayer))

object Posts:
  case class Add(user: String, post: Post)

  case class Post(@N(1) body: String)

  def all(user: String): ZStream[Feed, Err, (Eid, Post)] =
    for
      r <- kvs.feed.all(fid(user))
    yield r
  
  def fid(user: String): String = s"posts.$user"
  
  given Codec[Posts.Post] = caseCodecAuto
end Posts

class Posts(feed: Feed.Service) extends Actor:
  import Posts.{given, *}

  def receive: Receive =
    case Posts.Add(user, post) =>
      sender() ! Runtime.default.unsafeRunSync{
        for
          _ <- feed.add(fid(user), post)
        yield "added"
      }

    case _ =>
      sender() ! "bad msg"

end Posts

