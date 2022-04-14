package kvs.search

import java.util.Collections
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.Document
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.index.IndexWriterConfig.OpenMode
import org.apache.lucene.index.{IndexWriterConfig, IndexWriter, Term}
import org.apache.lucene.search.{IndexSearcher, Query}
import org.apache.lucene.store.Directory
import scala.reflect.ClassTag
import zio.*, stream.*

object Search:
  trait Service:
    val dirname: String
    def run[A](q: Query, `doc->a`: Document => Task[A], limit: Int=10)(using ClassTag[A]): ZStream[Any, Throwable, A]
    def index[R, E, A](xs: ZStream[R, E, A], `a->doc`: A => Document): ZIO[R, E | Throwable, Unit]
  end Service

  val live: ZLayer[Has[KvsDirectory], Throwable, Has[Service]] =
    ZLayer.fromService(dir =>
      new Service:
        val dirname = dir.dir

        def run[A](q: Query, `doc->a`: Document => Task[A], limit: Int)(using ClassTag[A]): ZStream[Any, Throwable, A] =
          for
            reader <- ZStream.managed(ZManaged.fromAutoCloseable(ZIO.effect(DirectoryReader.open(dir).nn)))
            searcher <- ZStream.fromEffect(ZIO.effect(IndexSearcher(reader)))
            x <- ZStream.fromIterableM(ZIO.effect(searcher.search(q, limit).nn.scoreDocs.nn))
            doc <- ZStream.fromEffect(ZIO.effect(searcher.doc(x.nn.doc).nn))
            a <- ZStream.fromEffect(`doc->a`(doc))
          yield a

        def index[R, E, A](xs: ZStream[R, E, A], `a->doc`: A => Document): ZIO[R, E | Throwable, Unit] =
          for
            a <- IO.effect(StandardAnalyzer()) //todo: managed
            c <- IO.effect(IndexWriterConfig(a))
            _ <- IO.effect(c.setOpenMode(OpenMode.CREATE))
            w <- IO.effect(IndexWriter(dir, c)) //todo: managed
            _ <- xs.mapM(a => ZIO.effect(w.addDocument(`a->doc`(a))): ZIO[R, E | Throwable, Unit]).runDrain
            _ <- IO.effect(w.close())
            _ <- IO.effect(a.close())
          yield ()
  )
end Search
