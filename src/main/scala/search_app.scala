package kvs.search
package app

import akka.actor.{Actor, Props}
import java.io.IOException
import kvs.feed.*
import kvs.rng.{ActorSystem, Dba}
import kvs.sharding.*
import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.document.{Document, StoredField}
import org.apache.lucene.index.Term
import org.apache.lucene.search.BooleanClause.Occur
import org.apache.lucene.search.{BooleanQuery, TermQuery}
import org.apache.lucene.store.Directory
import proto.*
import zio.*, stream.*, clock.*, console.*

case class PostsSearch(s: Search.Service[Post])
case class NotesSearch(s: Search.Service[Note])

@main
def searchApp: Unit =
  val io: ZIO[Has[PostsSearch] & Has[NotesSearch] & ClusterSharding & Console, Any, Unit] =
    for
      posts <- ZIO.service[PostsSearch].map(_.s)
      notes <- ZIO.service[NotesSearch].map(_.s)
      sharding <- ZIO.service[ClusterSharding.Service]
      shards <-
        sharding.start("Search", Props(Indexer(posts, notes)), {
          case _: IndexPosts => posts.dirname
          case _: IndexNotes => notes.dirname
        })
      //todo index
      _ <- putStrLn(s"welcome!")
      _ <- putStrLn(s"enter 'quit' to quit")
      _ <-
        (for
          _ <- putStrLn("search?")
          word <- getStrLn
          xs <- posts.run({
            val b = BooleanQuery.Builder()
            b.add(TermQuery(Term("title", word)), Occur.SHOULD)
            b.add(TermQuery(Term("content", word)), Occur.SHOULD)
            b.build
          }, doc => Post(doc.get("title"), doc.get("context")))
          _ <- putStrLn(xs.mkString("\n"))
          ys <- notes.run(TermQuery(Term("text", word)), doc => Note(doc.get("text")))
          _ <- putStrLn(ys.mkString("\n"))
        yield ()).repeatUntilEquals("quit")
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
  val postsDir: TaskLayer[Has[KvsDirectory]] =
    dba ++ ZLayer.succeed("posts") >>> KvsDirectory.live.fresh
  val notesDir: TaskLayer[Has[KvsDirectory]] =
    dba ++ ZLayer.succeed("notes") >>> KvsDirectory.live.fresh
  val postsSearch: TaskLayer[Has[PostsSearch]] =
    postsDir >>> Search.live().project(PostsSearch(_))
  val notesSearch: TaskLayer[Has[NotesSearch]] =
    notesDir >>> Search.live().project(NotesSearch(_))
  val shardingLayer: TaskLayer[ClusterSharding] =
    actorSystem >>> ClusterSharding.live
  
  Runtime.default.unsafeRun(io.provideCustomLayer(postsSearch ++ notesSearch ++ shardingLayer))

case class Post(@N(1) title: String, @N(2) content: String)
case class Note(@N(1) text: String)

case class IndexPosts(xs: ZStream[Any, Nothing, Post])
case class IndexNotes(xs: ZStream[Any, Nothing, Note])

class Indexer(posts: Search.Service[Post], notes: Search.Service[Note]) extends Actor:
  def receive: Receive =
    case IndexPosts(xs) =>
      sender() ! Runtime.default.unsafeRunSync{
        for
          _ <- posts.index(xs, ZIO.effectTotal(EnglishAnalyzer()), (p: Post) => {
              val doc = Document()
              doc.add(StoredField("title", p.title))
              doc.add(StoredField("content", p.content))
              doc
          })
        yield ()
      }

    case IndexNotes(xs) =>
      sender() ! Runtime.default.unsafeRunSync{
        for
          _ <- notes.index(xs, ZIO.effectTotal(EnglishAnalyzer()), (n: Note) => {
              val doc = Document()
              doc.add(StoredField("text", n.text))
              doc
          })
        yield ()
      }

    case _ =>
      sender() ! "bad msg"
end Indexer
