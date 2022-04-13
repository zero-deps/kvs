package kvs.search

import java.util.Collections
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.document.Document
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.index.IndexWriterConfig.OpenMode
import org.apache.lucene.index.{IndexWriterConfig, IndexWriter, Term}
import org.apache.lucene.search.{ScoreDoc, IndexSearcher, Query}
import org.apache.lucene.store.Directory
import scala.reflect.ClassTag
import zio.*, stream.*

object Search:
  def run[A](dir: Directory, q: Query, `doc->a`: Document => A, limit: Int=10)(using ClassTag[A]): Vector[A] =
    val r = DirectoryReader.open(dir)
    val s = IndexSearcher(r)
    s.search(q, limit).scoreDocs.map(x => `doc->a`(s.doc(x.doc))).toVector

  def unguarded_index[A](dir: Directory, xs: ZStream[Any, Nothing, A], analyzer: ZIO[Any, Nothing, Analyzer], `a->doc`: A => Document): ZIO[Any, Throwable, Unit] =
    (for
      a <- ZManaged.fromAutoCloseable(analyzer)
      c <- ZManaged.succeed(IndexWriterConfig(a))
      _ <- ZManaged.effectTotal(c.setOpenMode(OpenMode.CREATE))
      w <- ZManaged.fromAutoCloseable(ZIO.effectTotal(IndexWriter(dir, c)))
      _ <-
        ZManaged.fromEffect(
          xs
            .mapM(a =>
              for
                doc <- ZIO.effectTotal(`a->doc`(a))
                _ <- ZIO.effect(w.addDocument(doc))
              yield ()
            ).runDrain
        )
    yield ()).useNow
