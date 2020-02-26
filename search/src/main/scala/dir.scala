package zd.kvs
package search

import java.io.{IOException, ByteArrayOutputStream}
import java.nio.file.{NoSuchFileException, FileAlreadyExistsException}
import java.util.{Collection, Collections, Arrays}
import org.apache.lucene.store.{BaseDirectory, IndexOutput, IndexInput, FSLockFactory, IOContext, OutputStreamIndexOutput}
import scala.collection.mutable
import zd.kvs.en.{FdHandler}
import zd.kvs.file.FileHandler
import zd.gs.z._
import zd.proto.Bytes

class KvsDirectory(dir: Bytes)(kvs: Kvs) extends BaseDirectory(FSLockFactory.getDefault) {
  implicit val fileh = new FileHandler {
    override val chunkLength = 10 * 1000 * 1000 // 10 MB
  }
  implicit private[this] val fdh = FdHandler

  private[this] val outs = mutable.Map[String,ByteArrayOutputStream]()

  def exists: Res[Boolean] = {
    kvs.fd.get(dir).map(_.isDefined)
  }
  
  def deleteAll(): Res[Unit] = {
    for {
      xs <- kvs.all(dir)
      ys <- xs.sequence
      _ <- ys.map{ x =>
        val name = x.id
        for {
          _ <- kvs.file.delete(dir, name).void.recover{ case _: zd.kvs.FileNotExists => () }
          _ <- kvs.remove(dir, name)
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
    import zd.gs.z._
    kvs.all(dir).flatMap(_.sequence).fold(
      l => throw new IOException(l.toString)
    , r => r.map(x => new String(x.id.unsafeArray, "UTF-8")).sorted.toArray
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
      _ <- kvs.file.delete(dir, name1)
      x <- kvs.remove(dir, name1)
      _ <- kvs.cleanup(dir)
    } yield x.void
    res.fold(
      l => l match {
        case _: zd.kvs.FileNotExists => throw new NoSuchFileException(s"${dir}/${name}")
        case x => throw new IOException(x.toString)
      },
      r => r match {
        case Just(()) => ()
        case Nothing => throw new NoSuchFileException(s"${dir}/${name}")
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
    val name1 = Bytes.unsafeWrap(name.getBytes("UTF-8"))
    sync(Collections.singletonList(name))
    kvs.file.size(dir, name1).fold(
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
    val name1 = Bytes.unsafeWrap(name.getBytes("UTF-8"))
    val r = for {
      _ <- kvs.add(dir, name1, Bytes.unsafeWrap(Array.emptyByteArray))
      _ <- kvs.file.create(dir, name1)
    } yield ()
    r.fold(
      l => l match {
        case _: zd.kvs.EntryExists => throw new FileAlreadyExistsException(s"${dir}/${name}")
        case _: zd.kvs.FileAlreadyExists => throw new FileAlreadyExistsException(s"${dir}/${name}")
        case _ => throw new IOException(l.toString)
      },
      _ => {
        val out = new ByteArrayOutputStream;
        outs += name -> out
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
    val res = for {
      counter <- kvs.fd.length(dir) //todo: replace with loop
      name = s"${prefix}_${suffix}_${counter}.tmp"
      name1 = Bytes.unsafeWrap(name.getBytes("UTF-8"))
      _ <- kvs.add(dir, name1, Bytes.unsafeWrap(Array.emptyByteArray))
      _ <- kvs.file.create(dir, name1)
    } yield name
    res.fold(
      l => throw new IOException(l.toString),
      name => {
        val out = new ByteArrayOutputStream;
        outs += name -> out
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
    names.stream.forEach{ name =>
      outs.get(name).map(x => Bytes.unsafeWrap(x.toByteArray)).foreach{ xs =>
        val name1 = Bytes.unsafeWrap(name.getBytes("UTF-8"))
        kvs.file.append(dir, name1, xs).fold(
          l => throw new IOException(l.toString),
          _ => ()
        )
        outs -= name
      }
    }
  }

  override
  def syncMetaData(): Unit = ()

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
    val source1 = Bytes.unsafeWrap(source.getBytes("UTF-8"))
    val dest1 = Bytes.unsafeWrap(dest.getBytes("UTF-8"))
    sync(Arrays.asList(source, dest))
    val res = for {
      _ <- kvs.file.copy(dir, source1 -> dest1)
      _ <- kvs.add(dir, dest1, Bytes.unsafeWrap(Array.emptyByteArray))
      _ <- kvs.file.delete(dir, source1)
      _ <- kvs.remove(dir, source1)
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
    import zd.gs.z._
    sync(Collections.singletonList(name))
    val name1 = Bytes.unsafeWrap(name.getBytes("UTF-8"))
    val res = for {
      bs <- kvs.file.stream(dir, name1)
      bs1 <- bs.sequence
    } yield new BytesIndexInput(s"${dir}/${name}", bs1)
    res.fold(
      l => l match {
        case zd.kvs.FileNotExists(dir, name) => throw new NoSuchFileException(s"${dir}/${name}")
        case _ => throw new IOException(l.toString)
      },
      r => r
    )
  }

  override
  def close(): Unit = {
    isOpen = false
  }

  override
  def getPendingDeletions(): java.util.Set[String] = {
    Collections.emptySet[String]
  }
}
