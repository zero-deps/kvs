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
  trait Service[A]:
    val dirname: String
    def run[A](q: Query, `doc->a`: Document => A, limit: Int=10)(using ClassTag[A]): UIO[Vector[A]]
    def index[A, R1, R2, E1, E2](xs: ZStream[R1, E1, A], analyzer: ZIO[R2, E2, Analyzer], `a->doc`: A => Document): ZIO[R1 & R2, E1 | E2 | Throwable, Unit]
  end Service

  def live[A](): ZLayer[Has[KvsDirectory], Throwable, Has[Service[A]]] = ZLayer.fromService(dir =>
    new Service[A] {
      val dirname = dir.dir
      val indexReader = DirectoryReader.open(dir)
      val indexSearcher = IndexSearcher(indexReader)

      def run[A](q: Query, `doc->a`: Document => A, limit: Int)(using ClassTag[A]): UIO[Vector[A]] =
        ZIO.effectTotal(indexSearcher.search(q, limit).scoreDocs.map(x => `doc->a`(indexSearcher.doc(x.doc))).toVector)

      def index[A, R1, R2, E1, E2](xs: ZStream[R1, E1, A], analyzer: ZIO[R2, E2, Analyzer], `a->doc`: A => Document): ZIO[R1 & R2, E1 | E2 | Throwable, Unit] =
        (for
          a <- (ZManaged.fromAutoCloseable(analyzer): ZManaged[R1 & R2, E1 | E2 | Throwable, Analyzer])
          c <- ZManaged.succeed(IndexWriterConfig(a))
          _ <- ZManaged.effectTotal(c.setOpenMode(OpenMode.CREATE))
          w <- (ZManaged.fromAutoCloseable(ZIO.effectTotal(IndexWriter(dir, c))): ZManaged[R1 & R2, E1 | E2 | Throwable, IndexWriter])
          _ <-
            (ZManaged.fromEffect(
              (xs
                .mapM(a =>
                  (for
                    doc <- ZIO.effectTotal(`a->doc`(a))
                    _ <- ZIO.effect(w.addDocument(doc))
                  yield ()): ZIO[R1 & R2, E1 | E2 | Throwable, Unit]
                ).runDrain): ZIO[R1 & R2, E1 | E2 | Throwable, Unit]
            ): ZManaged[R1 & R2, E1 | E2 | Throwable, Unit])
        yield ()).useNow
    }
  )
end Search
