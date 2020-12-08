package kvs
package file

import zd.proto._, api._, macrosapi._
import zero.ext._, either._
import zio._, stream.Stream

import store.Dba

final case class File
  ( // count – number of chunks
    @N(1) count: Long
    // size - size of file in bytes
  , @N(2) size: Long
    // true if directory
  , @N(3) dir: Boolean
  )

trait FileHandler {
  protected val chunkLength: Int

  private implicit val filec = caseCodecAuto[File]

  private def get(path: PathKey)(implicit dba: Dba): KIO[File] = {
    dba.get(path).flatMap{
      case Some(x) => unpickle[File](x)
      case None    => IO.fail(FileNotExists(path))
    }
  }

  def create(path: PathKey)(implicit dba: Dba): KIO[File] = {
    dba.get(path).flatMap{
      case Some(_) => IO.fail(FileAlreadyExists(path))
      case None    =>
        val f = File(count=0, size=0L, dir=false)
        for {
          p <- pickle(f)
          _ <- dba.put(path, p)
        } yield f
    }
  }

  def append(path: PathKey, data: Bytes)(implicit dba: Dba): KIO[File] = {
    //todo: write all chunks in parallel
    def writeChunks(count: Long, rem: Bytes): KIO[Long] = {
      rem.splitAt(chunkLength) match {
        case (xs, _) if xs.length == 0 => IO.succeed(count)
        case (xs, ys) =>
          dba.put(ChunkKey(path, count+1), xs).flatMap(
            _ => writeChunks(count+1, rem=ys)
          )
      }
    }
    for {
      file  <- get(path)
      count <- writeChunks(file.count, rem=data)
      file1  = file.copy(count=count, size=file.size+data.length)
      p     <- pickle(file1)
      _     <- dba.put(path, p)
    } yield file1
  }

  def size(path: PathKey)(implicit dba: Dba): KIO[Long] = {
    get(path).map(_.size)
  }

  def stream(path: PathKey)(implicit dba: Dba): KStream[Bytes] = {
    Stream.fromEffect(get(path).map(_.count)).flatMap{
      case n if n < 0 => Stream.dieMessage("negative count")
      case 0 => Stream.empty
      case n if n > 0 =>
        def k(i: Long): ChunkKey = ChunkKey(path, i)
        Stream.fromIterable(LazyList.range(1, n+1)).mapM(i=>dba(k(i)))
    }
  }

  def delete(path: PathKey)(implicit dba: Dba): KIO[File] = {
    for {
      file <- get(path)
      _    <- Stream.fromIterable(LazyList.range(1, file.count+1)).mapM(i=>dba.del(ChunkKey(path, i))).runDrain
      _    <- dba.del(path)
    } yield file
  }

  def copy(fromPath: PathKey, toPath: PathKey)(implicit dba: Dba): KIO[File] = {
    for {
      from <- get(fromPath)
      _ <- get(toPath).fold(
        l => l match {
          case _: FileNotExists => ().right
          case _ => l.left
        },
        _ => FileAlreadyExists(toPath).left
      )
      _ <- Stream.fromIterable(LazyList.range(1, from.count+1)).mapM(i => for {
        x <- dba    (ChunkKey(fromPath, i))
        _ <- dba.put(ChunkKey(toPath  , i), x)
      } yield ()).runDrain
      to = File(from.count, from.size, from.dir)
      p <- pickle(to)
      _ <- dba.put(toPath, p)
    } yield to
  }

  implicit class BytesExt(x: Bytes) {
    def splitAt(n: Int): (Bytes, Bytes) = {
      val res = x.unsafeArray.splitAt(n)
      (Bytes.unsafeWrap(res._1), Bytes.unsafeWrap(res._2))
    }
  }
}
