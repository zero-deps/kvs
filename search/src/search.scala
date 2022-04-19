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
  def run[A : Codec : ClassTag](q: Query, limit: Int=10): ZStream[Any, Throwable, A]
  def index[R, E, A : Codec](xs: ZStream[R, E, A], `a->doc`: A => Document): ZIO[R, E | Throwable, Unit]
end Service

type Codec[A] = MessageCodec[A]

val live: ZLayer[Has[KvsDirectory], Throwable, Has[Service]] =
  ZLayer.fromService(dir =>
    new Service:
      val dirname = dir.dir

      def run[A : Codec : ClassTag](q: Query, limit: Int): ZStream[Any, Throwable, A] =
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

      def index[R, E, A : Codec](xs: ZStream[R, E, A], `a->doc`: A => Document): ZIO[R, E | Throwable, Unit] =
        Managed.fromAutoCloseable(IO.effect(StandardAnalyzer())).use{ a =>
          for
            c <- IO.effect(IndexWriterConfig(a))
            _ <- IO.effect(c.setOpenMode(OpenMode.CREATE))
            _ <-
              Managed.fromAutoCloseable(IO.effect(IndexWriter(dir, c))).use{ w =>
                xs.mapM(a => ZIO.effect(w.addDocument{
                  val doc = `a->doc`(a)
                  doc.add(StoredField("obj", encode[A](a)))
                  doc
                }): ZIO[R, E | Throwable, Unit]).runDrain
              }
          yield ()
        }
  )
