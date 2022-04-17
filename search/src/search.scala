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

trait Service:
  val dirname: String
  def run[A](q: Query, limit: Int=10)(using ClassTag[A], Codec[A]): ZStream[Any, Throwable, A]
  def index[R, E, A](xs: ZStream[R, E, A], `a->doc`: A => Document)(using Codec[A]): ZIO[R, E | Throwable, Unit]
end Service

type Codec[A] = MessageCodec[A]

val live: ZLayer[Has[KvsDirectory], Throwable, Has[Service]] =
  ZLayer.fromService(dir =>
    new Service:
      val dirname = dir.dir

      def run[A](q: Query, limit: Int)(using ClassTag[A], Codec[A]): ZStream[Any, Throwable, A] =
        for
          reader <- ZStream.managed(ZManaged.fromAutoCloseable(ZIO.effect(DirectoryReader.open(dir).nn)))
          searcher <- ZStream.fromEffect(ZIO.effect(IndexSearcher(reader)))
          x <- ZStream.fromIterableM(ZIO.effect(searcher.search(q, limit).nn.scoreDocs.nn))
          doc <- ZStream.fromEffect(ZIO.effect(searcher.doc(x.nn.doc).nn))
          a <-
            ZStream.fromEffect{
              for
                bs <- IO.effect(doc.getBinaryValue("obj").nn)
                obj <- IO.effect(decode[A](bs.bytes.nn))
              yield obj
            }
        yield a

      def index[R, E, A](xs: ZStream[R, E, A], `a->doc`: A => Document)(using Codec[A]): ZIO[R, E | Throwable, Unit] =
        for
          a <- IO.effect(StandardAnalyzer()) //todo: managed
          c <- IO.effect(IndexWriterConfig(a))
          _ <- IO.effect(c.setOpenMode(OpenMode.CREATE))
          w <- IO.effect(IndexWriter(dir, c)) //todo: managed
          _ <-
            xs.mapM(a => ZIO.effect(w.addDocument{
              val doc = `a->doc`(a)
              doc.add(StoredField("obj", encode[A](a)))
              doc
            }): ZIO[R, E | Throwable, Unit]).runDrain
          _ <- IO.effect(w.close())
          _ <- IO.effect(a.close())
        yield ()
  )
