package zd.kvs
package search

import java.io.{IOException, ByteArrayOutputStream}
import java.nio.file.{NoSuchFileException, FileAlreadyExistsException}
import java.util.{Collection, Collections, Arrays}
import java.util.concurrent.atomic.AtomicLong
import org.apache.lucene.store._
import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap
import zero.ext._, either._, option._, traverse._
import zd.kvs.en.FdHandler
import zd.kvs.file.FileHandler
import zd.proto.Bytes

class KvsDirectory(dir: FdKey)(kvs: Kvs) extends BaseDirectory(new KvsLockFactory(dir)) {
  implicit val fileh = new FileHandler {
    override val chunkLength = 10 * 1000 * 1000 // 10 MB
  }
  implicit private[this] val fdh = FdHandler

  private[this] val outs = TrieMap.empty[String,ByteArrayOutputStream]
  private[this] val nextTempFileCounter = new AtomicLong

  type File = Unit
  implicit object FileEntry extends Entry[File] {
    def fid(): FdKey = dir
    def extract(xs: Bytes): File = ()
    def insert(x: File): Bytes = Bytes.empty
  }

  def exists(): Res[Boolean] = {
    kvs.fd.get(dir).map(_.isDefined)
  }
  
  def deleteAll(): Res[Unit] = {
    for {
      xs <- kvs.all[File]()
      ys <- xs.sequence
      _ <- ys.map{ case (key, _) =>
        val name = key
        val path = PathKey(dir, name)
        for {
          _ <- kvs.file.delete(path).void.recover{ case _: zd.kvs.FileNotExists => () }
          _ <- kvs.remove[File](name)
        } yield ()
      }.sequence_
      _ <- kvs.cleanup(dir)
      _ <- kvs.fd.delete(dir)
    } yield ()
  }

  /**
   * Returns names of all files stored in this directory.
   * The output must be in sorted (UTF-16, java's {//link String#compareTo}) order.
   * 
   * //throws IOException in case of I/O error
   */
  override
  def listAll(): Array[String] = {
    ensureOpen()
    kvs.all[File]().flatMap(_.sequence).fold(
      l => throw new IOException(l.toString)
    , r => r.map(x => new String(x._1.unsafeArray, "UTF-8")).sorted.toArray
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
  def deleteFile(name: String): Unit = {
    val name1 = Bytes.unsafeWrap(name.getBytes("UTF-8"))
    sync(Collections.singletonList(name))
    val res: Res[Option[Unit]] = for {
      _ <- kvs.file.delete(PathKey(dir, name1))
      x <- kvs.remove[File](name1)
      _ <- kvs.cleanup(dir)
    } yield x.void
    res.fold(
      l => l match {
        case _: zd.kvs.FileNotExists => throw new NoSuchFileException(s"${dir}/${name}")
        case x => throw new IOException(x.toString)
      },
      r => r match {
        case Some(()) => ()
        case None => throw new NoSuchFileException(s"${dir}/${name}")
      }
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
  def fileLength(name: String): Long = {
    ensureOpen()
    val name1 = Bytes.unsafeWrap(name.getBytes("UTF-8"))
    sync(Collections.singletonList(name))
    kvs.file.size(PathKey(dir, name1)).fold(
      l => l match {
        case _: zd.kvs.FileNotExists => throw new NoSuchFileException(s"${dir}/${name}")
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
  def createOutput(name: String, context: IOContext): IndexOutput = {
    ensureOpen()
    val name1 = Bytes.unsafeWrap(name.getBytes("UTF-8"))
    val r = for {
      _ <- kvs.add[File](name1, ())
      _ <- kvs.file.create(PathKey(dir, name1))
    } yield ()
    r.fold(
      l => l match {
        case _: zd.kvs.EntryExists => throw new FileAlreadyExistsException(s"${dir}/${name}")
        case _: zd.kvs.FileAlreadyExists => throw new FileAlreadyExistsException(s"${dir}/${name}")
        case _ => throw new IOException(l.toString)
      },
      _ => {
        val out = new ByteArrayOutputStream;
        outs += ((name, out))
        new OutputStreamIndexOutput(s"${dir}/${name}", name, out, 8192)
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
  def createTempOutput(prefix: String, suffix: String, context: IOContext): IndexOutput = {
    ensureOpen()
    @tailrec def loop(): Res[String] = {
      val name = Directory.getTempFileName(prefix, suffix, nextTempFileCounter.getAndIncrement)
      val name1 = Bytes.unsafeWrap(name.getBytes("UTF-8"))
      val res = for {
        _ <- kvs.add[File](name1, ())
        _ <- kvs.file.create(PathKey(dir, name1))
      } yield name
      res match {
        case Left(_: EntryExists) => loop()
        case Left(_: FileAlreadyExists) => loop()
        case x => x
      }
    }
    val res = loop()
    res.fold(
      l => throw new IOException(l.toString),
      name => {
        val out = new ByteArrayOutputStream;
        outs += ((name, out))
        new OutputStreamIndexOutput(s"$dir/$name", name, out, 8192)
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
  def sync(names: Collection[String]): Unit = {
    ensureOpen()
    names.stream.forEach{ name =>
      outs.get(name).map(x => Bytes.unsafeWrap(x.toByteArray)).foreach{ xs =>
        val name1 = Bytes.unsafeWrap(name.getBytes("UTF-8"))
        kvs.file.append(PathKey(dir, name1), xs).fold(
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
  def rename(source: String, dest: String): Unit = {
    ensureOpen()
    val source1 = Bytes.unsafeWrap(source.getBytes("UTF-8"))
    val dest1 = Bytes.unsafeWrap(dest.getBytes("UTF-8"))
    sync(Arrays.asList(source, dest))
    val res = for {
      _ <- kvs.file.copy(from=PathKey(dir, source1), to=PathKey(dir, dest1))
      _ <- kvs.add[File](dest1, ())
      _ <- kvs.file.delete(PathKey(dir, source1))
      _ <- kvs.remove[File](source1)
      _ <- kvs.cleanup(dir)
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
  def openInput(name: String, context: IOContext): IndexInput = {
    sync(Collections.singletonList(name))
    val name1 = Bytes.unsafeWrap(name.getBytes("UTF-8"))
    val res = for {
      bs <- kvs.file.stream(PathKey(dir, name1))
      bs1 <- bs.sequence
    } yield new BytesIndexInput(s"${dir}/${name}", bs1)
    res.fold(
      l => l match {
        case zd.kvs.FileNotExists(path) => throw new NoSuchFileException(s"${path.dir}/${path.name}")
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
    Collections.emptySet[String]
  }
}

class KvsLockFactory(dir: FdKey) extends LockFactory {
  private[this] val locks = TrieMap.empty[Bytes, Unit]

  override def obtainLock(d: Directory, lockName: String): Lock = {
    val key = Bytes.unsafeWrap(dir.name.unsafeArray ++ lockName.getBytes("UTF-8"))
    locks.putIfAbsent(key, ()) match {
      case None => return new KvsLock(key)
      case Some(_) => throw new LockObtainFailedException(new String(key.unsafeArray, "UTF-8"))
    }
  }

  private[this] class KvsLock(key: Bytes) extends Lock {
    @volatile private[this] var closed = false

    override def ensureValid(): Unit = {
      if (closed) {
        throw new AlreadyClosedException(new String(key.unsafeArray, "UTF-8"))
      }
      if (!locks.contains(key)) {
        throw new AlreadyClosedException(new String(key.unsafeArray, "UTF-8"))
      }
    }

    override def close(): Unit = {
      locks -= key
      closed = true
    }
  }
}