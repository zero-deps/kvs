package kvs.search

import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.Document
import org.apache.lucene.document.StoredField
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.index.IndexWriterConfig.OpenMode
import org.apache.lucene.index.{IndexWriterConfig, IndexWriter}
import org.apache.lucene.search.{IndexSearcher, Query}
import proto.*
import scala.jdk.CollectionConverters.*
import scala.reflect.ClassTag
import zio.*, stream.*

trait Search:
  def run[A : Codec : ClassTag](q: Query, limit: Int=10): ZStream[Any, Throwable, A]
  def index[R, E, A : Codec](xs: ZStream[R, E, A], `a->doc`: A => Document): ZIO[R, E | Throwable, Unit]
end Search

case class SearchImpl(dir: KvsDirectory) extends Search:
  def run[A : Codec : ClassTag](q: Query, limit: Int): ZStream[Any, Throwable, A] =
    for
      reader <- ZStream.scoped(ZIO.fromAutoCloseable(ZIO.attempt(DirectoryReader.open(dir).nn)))
      searcher <- ZStream.fromZIO(ZIO.attempt(IndexSearcher(reader)))
      x <- ZStream.fromIterableZIO(ZIO.attempt(searcher.search(q, limit).nn.scoreDocs.nn))
      doc <- ZStream.fromZIO(ZIO.attempt(searcher.storedFields.nn.document(x.nn.doc, Set("obj").asJava).nn))
      a <- ZStream.fromZIO:
        for
          bs <- ZIO.attempt(doc.getBinaryValue("obj").nn)
          obj <- ZIO.attempt(decode[A](bs.bytes.nn))
        yield obj
    yield a

  def index[R, E, A : Codec](xs: ZStream[R, E, A], `a->doc`: A => Document): ZIO[R, E | Throwable, Unit] =
    ZIO.scoped:
      for
        a <- ZIO.fromAutoCloseable(ZIO.attempt(StandardAnalyzer()))
        c <- ZIO.attempt(IndexWriterConfig(a))
        _ <- ZIO.attempt(c.setOpenMode(OpenMode.CREATE))
        w <- ZIO.fromAutoCloseable(ZIO.attempt(IndexWriter(dir, c)))
        _ <-
          xs.mapZIO(a => ZIO.attempt(w.addDocument{
            val doc = `a->doc`(a)
            doc.add(StoredField("obj", encode[A](a)))
            doc
          }): ZIO[R, E | Throwable, Unit]).runDrain
      yield ()
end SearchImpl

val layer: URLayer[KvsDirectory, Search] =
  ZLayer.fromFunction(SearchImpl(_))
