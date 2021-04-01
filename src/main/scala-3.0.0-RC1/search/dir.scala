package zd.kvs
package search

import java.io.{IOException, ByteArrayOutputStream}
import java.nio.file.{NoSuchFileException, FileAlreadyExistsException}
import java.util.{Collection, Collections, Arrays}
import java.util.concurrent.atomic.AtomicLong
import org.apache.lucene.store.*
import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap
import scala.collection.JavaConverters.*
import zd.kvs.en.{Fd, feedHandler, EnHandler, FdHandler}
import zd.kvs.file.{File, FileHandler}

class KvsDirectory(dir: String)(kvs: Kvs) extends BaseDirectory(new KvsLockFactory(dir)) {
  implicit val fileh: FileHandler = new FileHandler {
    override val chunkLength = 10_000_000 // 10 MB
  }
  implicit private val indexFileHandler: EnHandler[IndexFile] = IndexFileHandler
  implicit private val fdh: FdHandler = feedHandler

  private val outs = TrieMap.empty[String,ByteArrayOutputStream]
  private val nextTempFileCounter = new AtomicLong

  def exists: Res[Boolean] = {
    kvs.fd.get(Fd(dir)).map(_.isDefined)
  }
  
  def deleteAll(): Res[Unit] = {
    for {
      xs <- kvs.all[IndexFile](dir)
      ys <- xs.sequence
      _  <- ys.map{ x =>
              val name = x.id
              for {
                _ <- kvs.file.delete(dir, name).map(_ => ()).recover{ case _: zd.kvs.FileNotExists => () }
                _ <- kvs.remove[IndexFile](dir, name)
              } yield ()
            }.sequence_
      _  <- kvs.fd.delete(zd.kvs.en.Fd(dir))(feedHandler).map(_ => ()).recover{ case _: zd.kvs.NotFound => () }
    } yield ()
  }

  /**
   * Returns names of all files stored in this directory.
   * The output must be in sorted (UTF-16, java's {//link String#compareTo}) order.
   * 
   * //throws IOException in case of I/O error
   */
  override
  def listAll(): Array[String | Null] | Null = {
    ensureOpen()
    kvs.all[IndexFile](dir).flatMap(_.sequence).fold(
      l => throw new IOException(l.toString)
    , r => r.map(_.id).sorted.toArray
    )
  }

  /**
   * Removes an existing file in the directory.
   *
   * This method must throw {//link NoSuchFileException}
   * if {@code name} points to a non-existing file.
   *
   * @param name the name of an existing file.
   * //throws IOException in case of I/O error
   */
  override
  def deleteFile(name: String | Null): Unit = {
    sync(Collections.singletonList(name.nn).nn)
    val r = for {
      _ <- kvs.file.delete(dir, name.nn)
      _ <- kvs.remove[IndexFile](dir, name.nn)
    } yield ()
    r.fold(
      l => l match {
        case _: zd.kvs.FileNotExists => throw new NoSuchFileException(s"${dir}/${name.nn}")
        case _: zd.kvs.NotFound => throw new NoSuchFileException(s"${dir}/${name.nn}")
        case x => throw new IOException(x.toString)
      },
      _ => ()
    )
  }

  /**
   * Returns the byte length of a file in the directory.
   *
   * This method must throw {//link NoSuchFileException}
   * if {@code name} points to a non-existing file.
   *
   * @param name the name of an existing file.
   * //throws IOException in case of I/O error
   */
  override
  def fileLength(name: String | Null): Long = {
    ensureOpen()
    sync(Collections.singletonList(name.nn).nn)
    kvs.file.size(dir, name.nn).fold(
      l => l match {
        case _: zd.kvs.FileNotExists => throw new NoSuchFileException(s"${dir}/${name.nn}")
        case _ => throw new IOException(l.toString)
      },
      r => r
    )
  }

  /**
   * Creates a new, empty file in the directory and returns an {//link IndexOutput}
   * instance for appending data to this file.
   *
   * This method must throw {//link java.nio.file.FileAlreadyExistsException} if the file
   * already exists.
   *
   * @param name the name of the file to create.
   * //throws IOException in case of I/O error
   */
  override
  def createOutput(name: String | Null, context: IOContext | Null): IndexOutput | Null = {
    ensureOpen()
    val r = for {
      _ <- kvs.add(IndexFile(dir, name.nn))
      _ <- kvs.file.create(dir, name.nn)
    } yield ()
    r.fold(
      l => l match {
        case _: zd.kvs.EntryExists => throw new FileAlreadyExistsException(s"${dir}/${name.nn}")
        case _: zd.kvs.FileAlreadyExists => throw new FileAlreadyExistsException(s"${dir}/${name.nn}")
        case _ => throw new IOException(l.toString)
      },
      _ => {
        val out = new ByteArrayOutputStream;
        outs += ((name.nn, out))
        new OutputStreamIndexOutput(s"${dir}/${name.nn}", name.nn, out, 8192)
      }
    )
  }

  /**
   * Creates a new, empty, temporary file in the directory and returns an {//link IndexOutput}
   * instance for appending data to this file.
   *
   * The temporary file name (accessible via {//link IndexOutput#getName()}) will start with
   * {@code prefix}, end with {@code suffix} and have a reserved file extension {@code .tmp}.
   */
  override
  def createTempOutput(prefix: String | Null, suffix: String | Null, context: IOContext | Null): IndexOutput | Null = {
    ensureOpen()
    @tailrec def loop(): Res[File] = {
      val name = Directory.getTempFileName(prefix, suffix, nextTempFileCounter.getAndIncrement).nn
      val res = for {
        _ <- kvs.add(IndexFile(dir, name))
        r <- kvs.file.create(dir, name)
      } yield r
      res match {
        case Left(_: EntryExists) => loop()
        case Left(_: FileAlreadyExists) => loop()
        case x => x
      }
    }
    val res = loop()
    res.fold(
      l => throw new IOException(l.toString),
      r => {
        val out = new ByteArrayOutputStream;
        outs += ((r.name, out))
        new OutputStreamIndexOutput(s"${dir}/${r.name}", r.name, out, 8192)
      }
    )
  }

  /**
   * Ensures that any writes to these files are moved to
   * stable storage (made durable).
   *
   * Lucene uses this to properly commit changes to the index, to prevent a machine/OS crash
   * from corrupting the index.
   */
  override
  def sync(names: Collection[String] | Null): Unit = {
    ensureOpen()
    names.nn.asScala.foreach{ (name: String) =>
      outs.get(name).map(_.toByteArray.nn).foreach{ xs =>
        kvs.file.append(dir, name, xs).fold(
          l => throw new IOException(l.toString),
          _ => ()
        )
        outs -= name
      }
    }
  }

  override
  def syncMetaData(): Unit = {
    ensureOpen()
    ()
  }

  /**
   * Renames {@code source} file to {@code dest} file where
   * {@code dest} must not already exist in the directory.
   *
   * It is permitted for this operation to not be truly atomic, for example
   * both {@code source} and {@code dest} can be visible temporarily in {//link #listAll()}.
   * However, the implementation of this method must ensure the content of
   * {@code dest} appears as the entire {@code source} atomically. So once
   * {@code dest} is visible for readers, the entire content of previous {@code source}
   * is visible.
   *
   * This method is used by IndexWriter to publish commits.
   */
  override
  def rename(source: String | Null, dest: String | Null): Unit = {
    ensureOpen()
    sync(Arrays.asList(source, dest).nn)
    val res = for {
      _ <- kvs.file.copy(dir, source.nn -> dest.nn)
      _ <- kvs.add(IndexFile(dir, dest.nn))
      _ <- kvs.file.delete(dir, source.nn)
      _ <- kvs.remove[IndexFile](dir, source.nn)
    } yield ()
    res.fold(
      l => throw new IOException(l.toString),
      _ => ()
    )
  }

  /**
   * Opens a stream for reading an existing file.
   *
   * This method must throw {//link NoSuchFileException}
   * if {@code name} points to a non-existing file.
   *
   * @param name the name of an existing file.
   * //throws IOException in case of I/O error
   */
  override
  def openInput(name: String | Null, context: IOContext | Null): IndexInput | Null = {
    sync(Collections.singletonList(name.nn).nn)
    val res = for {
      bs <- kvs.file.stream(dir, name.nn)
      bs1 <- bs.sequence
    } yield new BytesIndexInput(s"${dir}/${name.nn}", bs1)
    res.fold(
      l => l match {
        case zd.kvs.FileNotExists(dir, name) => throw new NoSuchFileException(s"${dir}/${name}")
        case _ => throw new IOException(l.toString)
      },
      r => r
    )
  }

  override def close(): Unit = synchronized {
    isOpen = false
  }

  override
  def getPendingDeletions(): java.util.Set[String] = {
    Collections.emptySet[String].nn
  }
}

class KvsLockFactory(dir: String) extends LockFactory {
  private val locks = TrieMap.empty[String, Unit]

  override def obtainLock(d: Directory | Null, lockName: String | Null): Lock | Null = {
    val key = dir + lockName
    locks.putIfAbsent(key, ()) match {
      case None => return new KvsLock(key)
      case Some(_) => throw new LockObtainFailedException(key)
    }
  }

  private class KvsLock(key: String) extends Lock {
    @volatile private var closed = false

    override def ensureValid(): Unit = {
      if (closed) {
        throw new AlreadyClosedException(key)
      }
      if (!locks.contains(key)) {
        throw new AlreadyClosedException(key)
      }
    }

    override def close(): Unit = {
      locks -= key
      closed = true
    }
  }
}
