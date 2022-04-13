package kvs.search

import java.io.{IOException, ByteArrayOutputStream}
import java.nio.file.{NoSuchFileException, FileAlreadyExistsException}
import java.util.concurrent.atomic.AtomicLong
import java.util.{Collection, Collections, Arrays}
import org.apache.lucene.store.*
import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap
import scala.collection.JavaConverters.*
import zio.*
import kvs.rng.Dba

object KvsDirectory:
  type DirName = String

  val live: ZLayer[Dba & Has[DirName], Nothing, Has[KvsDirectory]] = ZLayer.fromServices[Dba.Service, DirName, KvsDirectory]{ case (dba, dirname) =>
    KvsDirectory(dirname)(using DbaEff(dba))
  }

class KvsDirectory(val dir: String)(using DbaEff) extends BaseDirectory(NoLockFactory.INSTANCE):
  private val outs = TrieMap.empty[String, ByteArrayOutputStream]
  private val nextTempFileCounter = AtomicLong()

  /**
   * Returns names of all files stored in this directory.
   * The output must be in sorted (UTF-16, java's {@link String#compareTo}) order.
   * 
   * @throws IOException in case of I/O error
   */
  override
  def listAll(): Array[String | Null] | Null = {
    ensureOpen()
    Files.all(dir).fold(l => throw new IOException(l.toString), identity)
  }

  /**
   * Removes an existing file in the directory.
   *
   * This method must throw {@link NoSuchFileException}
   * if {@code name} points to a non-existing file.
   *
   * @param name the name of an existing file.
   * @throws IOException in case of I/O error
   */
  override
  def deleteFile(name: String | Null): Unit = {
    sync(Collections.singletonList(name.nn).nn)
    val r = for {
      _ <- File.delete(dir, name.nn)
      _ <- Files.remove(dir, name.nn)
    } yield ()
    r.fold(
      _ match
        case File.FileNotExists => throw new NoSuchFileException(name)
        case File.KeyNotFound => throw new NoSuchFileException(name)
        case x => throw new IOException(x.toString)
    , identity
    )
  }

  /**
   * Returns the byte length of a file in the directory.
   *
   * This method must throw {@link NoSuchFileException}
   * if {@code name} points to a non-existing file.
   *
   * @param name the name of an existing file.
   * @throws IOException in case of I/O error
   */
  override
  def fileLength(name: String | Null): Long = {
    ensureOpen()
    sync(Collections.singletonList(name.nn).nn)
    File.size(dir, name.nn).fold(
      l => l match {
        case File.FileNotExists => throw new NoSuchFileException(s"/search/dir/${dir}/${name.nn}")
        case _ => throw new IOException(l.toString)
      },
      r => r
    )
  }

  /**
   * Creates a new, empty file in the directory and returns an {@link IndexOutput}
   * instance for appending data to this file.
   *
   * This method must throw {@link java.nio.file.FileAlreadyExistsException} if the file
   * already exists.
   *
   * @param name the name of the file to create.
   * @throws IOException in case of I/O error
   */
  override
  def createOutput(name: String | Null, context: IOContext | Null): IndexOutput | Null = {
    ensureOpen()
    val r = for {
      _ <- Files.add(dir, name.nn)
      _ <- File.create(dir, name.nn)
    } yield ()
    r.fold(
      l => l match {
        case Files.EntryExists => throw new FileAlreadyExistsException(name)
        case File.FileAlreadyExists => throw new FileAlreadyExistsException(name)
        case _ => throw new IOException(l.toString)
      },
      _ => {
        val out = new ByteArrayOutputStream;
        outs += ((name.nn, out))
        new OutputStreamIndexOutput(s"/search/dir/${dir}/${name.nn}", name.nn, out, 8192)
      }
    )
  }

  /**
   * Creates a new, empty, temporary file in the directory and returns an {@link IndexOutput}
   * instance for appending data to this file.
   *
   * The temporary file name (accessible via {@link IndexOutput#getName()}) will start with
   * {@code prefix}, end with {@code suffix} and have a reserved file extension {@code .tmp}.
   */
  override
  def createTempOutput(prefix: String | Null, suffix: String | Null, context: IOContext | Null): IndexOutput | Null = {
    ensureOpen()
    @tailrec
    def loop(): Either[?, File] =
      val name = Directory.getTempFileName(prefix, suffix, nextTempFileCounter.getAndIncrement).nn
      val res =
        for
          _ <- Files.add(dir, name)
          r <- File.create(dir, name)
        yield r
      res match
        case Left(Files.EntryExists) => loop()
        case Left(File.FileAlreadyExists) => loop()
        case x => x

    val res = loop()
    res.fold(
      l => throw new IOException(l.toString),
      r => {
        val out = new ByteArrayOutputStream;
        outs += ((r.name, out))
        new OutputStreamIndexOutput(s"/search/dir/${dir}/${r.name}", r.name, out, 8192)
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
        File.append(dir, name, xs).fold(
          l => throw new IOException(l.toString),
          _ => ()
        )
        outs -= name
      }
    }
  }

  override
  def syncMetaData(): Unit =
    ensureOpen()

  /**
   * Renames {@code source} file to {@code dest} file where
   * {@code dest} must not already exist in the directory.
   *
   * It is permitted for this operation to not be truly atomic, for example
   * both {@code source} and {@code dest} can be visible temporarily in {@link #listAll()}.
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
      _ <- File.copy(dir, source.nn -> dest.nn)
      _ <- Files.add(dir, dest.nn)
      _ <- File.delete(dir, source.nn)
      _ <- Files.remove(dir, source.nn)
    } yield ()
    res.fold(
      l => throw new IOException(l.toString),
      _ => ()
    )
  }

  /**
   * Opens a stream for reading an existing file.
   *
   * This method must throw {@link NoSuchFileException}
   * if {@code name} points to a non-existing file.
   *
   * @param name the name of an existing file.
   * @throws IOException in case of I/O error
   */
  override
  def openInput(name: String | Null, context: IOContext | Null): IndexInput | Null = {
    sync(Collections.singletonList(name.nn).nn)
    val res = for {
      bs <- File.stream(dir, name.nn)
    } yield new ByteBuffersIndexInput(ByteBuffersDataInput(bs), s"/search/dir/${dir}/${name.nn}")
    res.fold(
      l => l match {
        case File.FileNotExists => throw new NoSuchFileException(name)
        case _ => throw new IOException(l.toString)
      },
      r => r
    )
  }

  override def close(): Unit =
    synchronized {
      isOpen = false
    }

  override
  def getPendingDeletions(): java.util.Set[String] =
    Collections.emptySet[String].nn
