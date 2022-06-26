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
import zio.*, stream.*
import zio.Console.{printLine, readLine}

case class PostsSearch(s: Search)
case class NotesSearch(s: Search)

object SearchApp extends ZIOAppDefault:
  def run =
    println("starting...")
    val io: ZIO[PostsSearch & NotesSearch & SeqConsistency, Any, Unit] =
      for
        posts <- ZIO.service[PostsSearch].map(_.s)
        notes <- ZIO.service[NotesSearch].map(_.s)
        seqc <- ZIO.service[SeqConsistency]
        _ <- printLine("indexing...")
        _ <- seqc.send(IndexPosts).flatMap(x => printLine(x.toString))
        _ <- seqc.send(IndexNotes).flatMap(x => printLine(x.toString))
        _ <- printLine("welcome!")
        _ <- printLine("enter 'q' to quit")
        _ <-
          (for
            _ <- printLine("search?")
            word <- readLine
            _ <-
              if word == "q" then ZIO.unit
              else
                for
                  xs <-
                    posts.run[Post]{
                      val b = BooleanQuery.Builder()
                      b.add(WildcardQuery(Term("title", s"*${word}*")), Occur.SHOULD)
                      b.add(WildcardQuery(Term("content", s"*${word}*")), Occur.SHOULD)
                      b.build.nn
                    }.runCollect
                  _ <- printLine("posts> " + xs)
                  ys <-
                    notes.run[Note](
                      WildcardQuery(Term("text", s"*${word}*"))
                    ).runCollect
                  _ <- printLine("notes> " + ys)
                yield ()
          yield word).repeatUntilEquals("q")
      yield ()

    val akkaConfig: ULayer[ActorSystem.Conf] =
      val name = "app"
      ActorSystem.staticConf(name, kvs.rng.akkaConf(name, "127.0.0.1", 4343) ++ "akka.loglevel=off")
    val dbaConfig: ULayer[kvs.rng.Conf] =
      ZLayer.succeed(kvs.rng.Conf(dir = "target/data"))
    val postsDir: URLayer[Dba, KvsDirectory] =
      ZLayer.succeed("posts_index") >>> KvsDirectory.live.fresh
    val notesDir: URLayer[Dba, KvsDirectory] =
      ZLayer.succeed("notes_index") >>> KvsDirectory.live.fresh
    val postsSearch: URLayer[Dba, PostsSearch] =
      postsDir >>> kvs.search.layer.fresh.project(PostsSearch(_))
    val notesSearch: URLayer[Dba, NotesSearch] =
      notesDir >>> kvs.search.layer.fresh.project(NotesSearch(_))
    val seqConsistencyConfig: URLayer[PostsSearch & NotesSearch, SeqConsistency.Config] =
      ZLayer(
        for
          posts <- ZIO.service[PostsSearch]
          notes <- ZIO.service[NotesSearch]
        yield
          SeqConsistency.Config(
            "Search"
          , {
              case IndexPosts =>
                (for
                  _ <-
                    posts.s.index[Any, Nothing, Post](
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
                yield "posts are indexed")

              case IndexNotes =>
                (for
                  _ <-
                    notes.s.index[Any, Nothing, Note](
                      ZStream(
                        Note("Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.")
                      , Note("Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.")
                      )
                    , n => {
                      val doc = Document()
                      doc.add(TextField("text", n.text, Field.Store.NO))
                      doc
                    })
                yield "notes are indexed")
            }
          , {
              case IndexPosts => "posts"
              case IndexNotes => "notes"
            }
          )
      )
    
    io.provide(
      SeqConsistency.live
    , seqConsistencyConfig
    , postsSearch
    , notesSearch
    , kvs.sharding.live
    , Dba.live
    , dbaConfig
    , ActorSystem.live
    , akkaConfig
    )
end SearchApp

case class Post(@N(1) title: String, @N(2) content: String)
case class Note(@N(1) text: String)

given Codec[Post] = caseCodecAuto
given Codec[Note] = caseCodecAuto

case object IndexPosts
case object IndexNotes

given CanEqual[IndexPosts.type, Any] = CanEqual.derived
given CanEqual[IndexNotes.type, Any] = CanEqual.derived

