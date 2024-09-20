package sort

import kvs.*, sort.*
import proto.*
import zd.rng.*
import zio.*, stream.*
import zio.Console.{printLine, readLine}

object SortApp extends ZIOAppDefault:
  def run =
    val io: ZIO[Sort & SeqConsistency, Any, Unit] =
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
                            answer <- seqc.send(Add(post))
                            _ <- printLine(answer.toString)
                          yield ()
                  yield ()
                case "all" =>
                  all().take(5).tap(x => printLine(x.body + "\n" + "-" * 10)).runDrain
                case _ => ZIO.unit
          yield s).repeatUntilEquals("q")
      yield ()

    val pekkoConfig: ULayer[ActorSystem.Conf] =
      val name = "app"
      ActorSystem.staticConf(name, zd.rng.pekkoConf(name, "127.0.0.1", 4343) ++ "pekko.loglevel=off")
    val dbaConfig: ULayer[zd.rng.Conf] =
      ZLayer.succeed(zd.rng.Conf(dir = "target/data"))
    val seqConsistencyConfig: URLayer[Sort, SeqConsistency.Config] =
      ZLayer.fromFunction((sort: Sort) =>
        SeqConsistency.Config(
          "Posts"
        , {
            case Add(post) =>
              (for
                _ <- kvs.sort.insert(ns, post)
              yield "added").provideLayer(ZLayer.succeed(sort))
          }
        , _ => "shard"
        )
      )
    
    io.provide(
      SeqConsistency.layer
    , seqConsistencyConfig
    , ClusterSharding.layer
    , SortImpl.layer
    , Rng.layer
    , dbaConfig
    , ActorSystem.layer
    , pekkoConfig
    )
end SortApp

case class Post(@N(1) body: String)

case class Add(post: Post)

def all(): ZStream[Sort, Nothing, Post] =
  kvs.sort.flatten(ns)

val ns = "posts_sorted"

given Codec[Post] = caseCodecAuto
given Ordering[Post] = Ordering.by(_.body)
given CanEqual[Post, Post] = CanEqual.derived
given CanEqual[None.type, Option[kvs.sort.Node[Post]]] = CanEqual.derived
