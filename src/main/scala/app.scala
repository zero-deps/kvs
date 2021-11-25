import akka.actor.{Actor, Props}
import java.io.IOException
import kvs.feed.*, sharding.*
import kvs.rng.{ActorSystem, Dba}
import proto.*
import zio.*, stream.*, clock.*, console.*

@main
def app: Unit =
  val io: ZIO[Feed & ClusterSharding & Console, Any, Unit] =
    for
      feed <- ZIO.service[Feed.Service]
      sharding <- ZIO.service[ClusterSharding.Service]
      posts <-
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
                          _ <- sharding.send[Unit](posts, Posts.Add(user, post))
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

  def all(user: String): ZStream[Feed, Err, (Eid, Post)] = kvs.feed.all(fid(user))
  
  def fid(user: String): String = s"posts.$user"
  
  given Codec[Posts.Post] = caseCodecAuto
end Posts

class Posts(feed: Feed.Service) extends WriteService(feed):
  import Posts.{given, *}

  val f: Any => IO[Err, Any] =
    case Posts.Add(user, post) =>
      for
        _ <- feed.add(fid(user), post)
      yield ()

    case _ => ZIO.dieMessage("unexpected message")
end Posts

