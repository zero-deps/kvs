package kvs.search

import java.util.Collections
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.Document
import org.apache.lucene.document.StoredField
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.index.IndexWriterConfig.OpenMode
import org.apache.lucene.index.{IndexWriterConfig, IndexWriter, Term}
import org.apache.lucene.search.{IndexSearcher, Query}
import org.apache.lucene.store.Directory
import proto.*
import scala.reflect.ClassTag
import zio.*, stream.*

trait Search:
  val dirname: String
  def run[A : Codec : ClassTag](q: Query, limit: Int=10): ZStream[Any, Throwable, A]
  def index[R, E, A : Codec](xs: ZStream[R, E, A], `a->doc`: A => Document): ZIO[R, E | Throwable, Unit]
end Search

type Codec[A] = MessageCodec[A]

val live: ZLayer[KvsDirectory, Nothing, Search] =
  ZLayer(
    for
      dir <- ZIO.service[KvsDirectory]
    yield
      new Search:
        val dirname = dir.dir

        def run[A : Codec : ClassTag](q: Query, limit: Int): ZStream[Any, Throwable, A] =
          for
            reader <- ZStream.scoped(ZIO.fromAutoCloseable(ZIO.attempt(DirectoryReader.open(dir).nn)))
            searcher <- ZStream.fromZIO(ZIO.attempt(IndexSearcher(reader)))
            x <- ZStream.fromIterableZIO(ZIO.attempt(searcher.search(q, limit).nn.scoreDocs.nn))
            doc <- ZStream.fromZIO(ZIO.attempt(searcher.doc(x.nn.doc).nn))
            a <-
              ZStream.fromZIO{
                for
                  bs <- IO.attempt(doc.getBinaryValue("obj").nn)
                  obj <- IO.attempt(decode[A](bs.bytes.nn))
                yield obj
              }
          yield a

        def index[R, E, A : Codec](xs: ZStream[R, E, A], `a->doc`: A => Document): ZIO[R, E | Throwable, Unit] =
          ZIO.scoped(
            for
              a <- ZIO.fromAutoCloseable(IO.attempt(StandardAnalyzer()))
              c <- IO.attempt(IndexWriterConfig(a))
              _ <- IO.attempt(c.setOpenMode(OpenMode.CREATE))
              w <- ZIO.fromAutoCloseable(IO.attempt(IndexWriter(dir, c)))
              _ <-
                xs.mapZIO(a => ZIO.attempt(w.addDocument{
                  val doc = `a->doc`(a)
                  doc.add(StoredField("obj", encode[A](a)))
                  doc
                }): ZIO[R, E | Throwable, Unit]).runDrain
            yield ()
          )
  )
