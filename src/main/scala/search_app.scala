package kvs.search
package app

import akka.actor.{Actor, Props}
import java.io.IOException
import kvs.feed.*
import kvs.rng.{ActorSystem, Dba}
import kvs.sharding.*
import org.apache.lucene.document.{Document, TextField, Field}
import org.apache.lucene.index.Term
import org.apache.lucene.search.BooleanClause.Occur
import org.apache.lucene.search.{BooleanQuery, WildcardQuery}
import org.apache.lucene.store.Directory
import proto.*
import zio.*, stream.*, clock.*, console.*

case class PostsSearch(s: kvs.search.Service)
case class NotesSearch(s: kvs.search.Service)

@main
def searchApp: Unit =
  println("starting...")
  val io: ZIO[Has[PostsSearch] & Has[NotesSearch] & ClusterSharding & Console, Any, Unit] =
    for
      posts <- ZIO.service[PostsSearch].map(_.s)
      notes <- ZIO.service[NotesSearch].map(_.s)
      sharding <- ZIO.service[kvs.sharding.Service]
      shards <-
        sharding.start("Search", Props(Indexer(posts, notes)), {
          case IndexPosts => posts.dirname
          case IndexNotes => notes.dirname
        })
      _ <- putStrLn("indexing...")
      _ <- sharding.send[String, Throwable](shards, IndexPosts).flatMap(putStrLn(_))
      _ <- sharding.send[String, Throwable](shards, IndexNotes).flatMap(putStrLn(_))
      _ <- putStrLn(s"welcome!")
      _ <- putStrLn(s"enter 'q' to quit")
      _ <-
        (for
          _ <- putStrLn("search?")
          word <- getStrLn
          _ <-
            if word == "q" then IO.unit
            else
              for
                xs <-
                  posts.run[Post]{
                    val b = BooleanQuery.Builder()
                    b.add(WildcardQuery(Term("title", s"*${word}*")), Occur.SHOULD)
                    b.add(WildcardQuery(Term("content", s"*${word}*")), Occur.SHOULD)
                    b.build.nn
                  }.runCollect
                _ <- putStrLn("posts> " + xs)
                ys <-
                  notes.run[Note](
                    WildcardQuery(Term("text", s"*${word}*"))
                  ).runCollect
                _ <- putStrLn("notes> " + ys)
              yield ()
        yield word).repeatUntilEquals("q")
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
    postsDir >>> kvs.search.live.fresh.project(PostsSearch(_))
  val notesSearch: TaskLayer[Has[NotesSearch]] =
    notesDir >>> kvs.search.live.fresh.project(NotesSearch(_))
  val shardingLayer: TaskLayer[ClusterSharding] =
    actorSystem >>> kvs.sharding.live
  
  Runtime.default.unsafeRun(io.provideCustomLayer(postsSearch ++ notesSearch ++ shardingLayer))

case class Post(@N(1) title: String, @N(2) content: String)
case class Note(@N(1) text: String)

type Codec[A] = MessageCodec[A]
given Codec[Post] = caseCodecAuto
given Codec[Note] = caseCodecAuto

case object IndexPosts
case object IndexNotes

given CanEqual[IndexPosts.type, Any] = CanEqual.derived
given CanEqual[IndexNotes.type, Any] = CanEqual.derived

class Indexer(posts: kvs.search.Service, notes: kvs.search.Service) extends Actor:
  def receive: Receive =
    case IndexPosts =>
      sender() ! Runtime.default.unsafeRunSync[Throwable, String]{
        for
          _ <-
            posts.index[Any, Nothing, Post](
              ZStream(
                Post("What is Lorem Ipsum?", "Lorem Ipsum is simply dummy text of the printing and typesetting industry.")
              , Post("Where does it come from?", "It has roots in a piece of classical Latin literature from 45 BC, making it over 2000 years old.")
              )
            , p => {
              val doc = Document()
              doc.add(TextField("title", p.title, Field.Store.NO))
              doc.add(TextField("content", p.content, Field.Store.NO))
              doc
            })
        yield "posts are indexed"
      }

    case IndexNotes =>
      sender() ! Runtime.default.unsafeRunSync[Throwable, String]{
        for
          _ <-
            notes.index[Any, Nothing, Note](
              ZStream(
                Note("Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.")
              , Note("Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.")
              )
            , n => {
              val doc = Document()
              doc.add(TextField("text", n.text, Field.Store.NO))
              doc
            })
        yield "notes are indexed"
      }

    case _ =>
      sender() ! "bad msg"
end Indexer
