package kvs
package search

import java.io.{IOException, ByteArrayOutputStream}
import java.nio.file.{NoSuchFileException, FileAlreadyExistsException}
import java.util.{Collection, Collections, Arrays}
import java.util.concurrent.atomic.AtomicLong
import org.apache.lucene.store._
import scala.collection.concurrent.TrieMap
import zero.ext._, boolean._, option._
import proto.Bytes
import zio._, stream.Stream

import file.FileHandler, store.Dba

class KvsDirectory(dir: FdKey)(implicit dba: Dba) extends BaseDirectory(new KvsLockFactory(dir)) {
  val flh = new FileHandler {
    override val chunkLength = 10_000_000 // 10 MB
  }
  val enh = feed
  val fdh = feed.meta

  val runtime: Runtime[Any] = Runtime.default

  private val outs = TrieMap.empty[String,ByteArrayOutputStream]
  private val nextTempFileCounter = new AtomicLong

  // def exists(): IO[Err, Boolean] = {
  //   fdh.get(dir).map(_.isDefined)
  // }

  // def deleteAll(): Res[Unit] = {
  //   for {
  //     xs <- enh.all(dir)
  //     ys <- xs.sequence
  //     _ <- ys.map{ case (key, _) =>
  //       val name = key
  //       val path = PathKey(dir, name)
  //       for {
  //         _ <- flh.delete(path).void.recover{ case _: FileNotExists => () }
  //         _ <- enh.remove(EnKey(dir, name))
  //       } yield ()
  //     }.sequence_
  //     _ <- enh.cleanup(dir)
  //     _ <- fdh.delete(dir)
  //   } yield ()
  // }

  /**
   * Returns names of all files stored in this directory.
   * The output must be in sorted (UTF-16, java's {//link String#compareTo}) order.
   * 
   * //throws IOException in case of I/O error
   */
  override def listAll(): Array[String] = {
    ensureOpen()
    val z = enh.all(dir).map(_._1.bytes.mkString).runCollect.map(_.sorted.toArray)
    runtime.unsafeRunSync(z.provideLayer(ZEnv.live)).getOrElse(e => throw new IOException(FiberFailure(e)))
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
  override def deleteFile(name: String): Unit = {
    sync(Collections.singletonList(name))
    val z = for {
      name1 <- ElKeyExt.from_str(name)
      path  <- IO.succeed(PathKey(dir, name1))
      _     <- flh.delete(path)
      x     <- enh.remove(EnKey(dir, name1))
      _     <- x.fold(enh.cleanup(dir), IO.fail(FileNotExists(path)))
    } yield ()
    runtime.unsafeRunSync(z.provideLayer(ZEnv.live)).getOrElse(c => throw c.failureOption.collect{
      case _: FileNotExists => new NoSuchFileException(s"${dir}/${name}")
    }.getOrElse(new IOException(FiberFailure(c))))
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
  override def fileLength(name: String): Long = {
    ensureOpen()
    sync(Collections.singletonList(name))
    val z = for {
      name1 <- ElKeyExt.from_str(name)
      l     <- flh.size(PathKey(dir, name1))
    } yield l
    runtime.unsafeRunSync(z.provideLayer(ZEnv.live)).getOrElse(c => throw c.failureOption.collect{
      case _: FileNotExists => new NoSuchFileException(s"${dir}/${name}")
    }.getOrElse(new IOException(FiberFailure(c))))
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
  override def createOutput(name: String, context: IOContext): IndexOutput = {
    ensureOpen()
    val z = for {
      name1 <- ElKeyExt.from_str(name)
      _     <- enh.putIfAbsent(EnKey(dir, name1), Bytes.empty)
      _     <- flh.create(PathKey(dir, name1))
      out   <- IO.effectTotal(new ByteArrayOutputStream)
      _     <- IO.effectTotal(outs += ((name, out)))
      io    <- IO.effectTotal(new OutputStreamIndexOutput(s"${dir}/${name}", name, out, 8192))
    } yield io
    runtime.unsafeRunSync(z.provideLayer(ZEnv.live)).getOrElse(c => throw c.failureOption.collect{
      case _: EntryExists       => new FileAlreadyExistsException(s"${dir}/${name}")
      case _: FileAlreadyExists => new FileAlreadyExistsException(s"${dir}/${name}")
    }.getOrElse(new IOException(FiberFailure(c))))
  }

  /**
   * Creates a new, empty, temporary file in the directory and returns an {//link IndexOutput}
   * instance for appending data to this file.
   *
   * The temporary file name (accessible via {//link IndexOutput#getName()}) will start with
   * {@code prefix}, end with {@code suffix} and have a reserved file extension {@code .tmp}.
   */
  override def createTempOutput(prefix: String, suffix: String, context: IOContext): IndexOutput = {
    ensureOpen()
    val z = (for {
      i     <- IO.effectTotal(nextTempFileCounter.getAndIncrement)
      name  <- IO.effect(Directory.getTempFileName(prefix, suffix, i)).orDie
      name1 <- ElKeyExt.from_str(name)
      _     <- enh.putIfAbsent(EnKey(dir, name1), Bytes.empty)
      _     <- flh.create(PathKey(dir, name1))
      out   <- IO.effectTotal(new ByteArrayOutputStream)
      _     <- IO.effectTotal(outs += ((name, out)))
      io    <- IO.effectTotal(new OutputStreamIndexOutput(s"$dir/$name", name, out, 8192))
    } yield io).retryWhile{
      case _: EntryExists       => true
      case _: FileAlreadyExists => true
      case _ => false
    }
    runtime.unsafeRunSync(z.provideLayer(ZEnv.live)).getOrElse(c => throw new IOException(FiberFailure(c)))
  }

  /**
   * Ensures that any writes to these files are moved to
   * stable storage (made durable).
   *
   * Lucene uses this to properly commit changes to the index, to prevent a machine/OS crash
   * from corrupting the index.
   */
  override def sync(names: Collection[String]): Unit = {
    ensureOpen()
    val z = Stream.fromJavaIterator(names.iterator).mapM{ name =>
      for {
        out  <- IO.effectTotal(outs.get(name))
        _    <- out.cata(out =>
                  for {
                    bs    <- IO.effectTotal(Bytes.unsafeWrap(out.toByteArray))
                    name1 <- ElKeyExt.from_str(name)
                    _     <- flh.append(PathKey(dir, name1), bs)
                    _     <- IO.effectTotal(outs -= name)
                  } yield ()
                , IO.unit
                )
      } yield ()
    }.runDrain
    runtime.unsafeRunSync(z.provideLayer(ZEnv.live)).getOrElse(c => throw new IOException(FiberFailure(c)))
  }

  override def syncMetaData(): Unit = {
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
  override def rename(source: String, dest: String): Unit = {
    ensureOpen()
    sync(Arrays.asList(source, dest))
    val z = for {
      source1 <- ElKeyExt.from_str(source)
      dest1   <- ElKeyExt.from_str(dest)
      _       <- flh.copy(fromPath=PathKey(dir, source1), toPath=PathKey(dir, dest1))
      _       <- enh.putIfAbsent(EnKey(dir, dest1), Bytes.empty)
      _       <- flh.delete(PathKey(dir, source1))
      _       <- enh.remove(EnKey(dir, source1))
      _       <- enh.cleanup(dir)
    } yield ()
    runtime.unsafeRunSync(z.provideLayer(ZEnv.live)).getOrElse(c => throw new IOException(FiberFailure(c)))
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
  override def openInput(name: String, context: IOContext): IndexInput = {
    sync(Collections.singletonList(name))
    val z = for {
      name1 <- ElKeyExt.from_str(name)
      bs    <- flh.stream(PathKey(dir, name1)).runCollect
    } yield new BytesIndexInput(s"${dir}/${name}", bs.toVector)
    runtime.unsafeRunSync(z.provideLayer(ZEnv.live)).getOrElse(c => throw c.failureOption.collect{
      case FileNotExists(path) => new NoSuchFileException(s"${path.dir}/${path.name}")
    }.getOrElse(new IOException(FiberFailure(c))))
  }

  override def close(): Unit = synchronized {
    isOpen = false
  }

  override def getPendingDeletions(): java.util.Set[String] = {
    Collections.emptySet[String]
  }
}

class KvsLockFactory(dir: FdKey) extends LockFactory {
  private[this] val locks = TrieMap.empty[Bytes, Unit]

  override def obtainLock(d: Directory, lockName: String): Lock = {
    val key = Bytes.unsafeWrap(dir.bytes.unsafeArray ++ lockName.getBytes("UTF-8"))
    locks.putIfAbsent(key, ()) match {
      case None => return new KvsLock(key)
      case Some(_) => throw new LockObtainFailedException(key.mkString)
    }
  }

  private[this] class KvsLock(key: Bytes) extends Lock {
    @volatile private[this] var closed = false

    override def ensureValid(): Unit = {
      if (closed) {
        throw new AlreadyClosedException(key.mkString)
      }
      if (!locks.contains(key)) {
        throw new AlreadyClosedException(key.mkString)
      }
    }

    override def close(): Unit = {
      locks -= key
      closed = true
    }
  }
}
