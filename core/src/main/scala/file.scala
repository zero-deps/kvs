package zd.kvs
package file

import zd.kvs.store.Dba
import scala.annotation.tailrec
import zd.proto._, api._, macrosapi._
import zero.ext._, either._, boolean._, option._, traverse._

final case class File
  ( // count – number of chunks
    @N(1) count: Int
    // size - size of file in bytes
  , @N(2) size: Long
    // true if directory
  , @N(3) dir: Boolean
  )

trait FileHandler {
  protected val chunkLength: Int

  private implicit val filec = caseCodecAuto[File]

  private def get(path: PathKey)(implicit dba: Dba): Res[File] = {
    dba.get(path) match {
      case Right(Some(x)) => unpickle[File](x).right
      case Right(None) => FileNotExists(path).left
      case x@Left(_) => x.coerceRight
    }
  }

  def create(path: PathKey)(implicit dba: Dba): Res[File] = {
    dba.get(path) match {
      case Right(Some(_)) => FileAlreadyExists(path).left
      case Right(None) =>
        val f = File(count=0, size=0L, dir=false)
        for {
          _ <- dba.put(path, pickle(f))
        } yield f
      case x@Left(_) => x.coerceRight
    }
  }

  def append(path: PathKey, data: Bytes)(implicit dba: Dba): Res[File] = {
    @tailrec
    def writeChunks(count: Int, rem: Bytes): Res[Int] = {
      rem.splitAt(chunkLength) match {
        case (xs, _) if xs.length == 0 => count.right
        case (xs, ys) =>
          dba.put(ChunkKey(path, count+1), xs) match {
            case r @ Right(_) => writeChunks(count+1, rem=ys)
            case l @ Left(_) => l.coerceRight
          }
      }
    }
    for {
      _ <- (data.length == 0).fold(Fail("data is empty").left, ().right)
      file <- get(path)
      count <- writeChunks(file.count, rem=data)
      file1 = file.copy(count=count, size=file.size+data.length)
      _ <- dba.put(path, pickle(file1))
    } yield file1
  }

  def size(path: PathKey)(implicit dba: Dba): Res[Long] = {
    get(path).map(_.size)
  }

  def stream(path: PathKey)(implicit dba: Dba): Res[LazyList[Res[Bytes]]] = {
    get(path).map(_.count).flatMap{
      case n if n < 0 => Fail(s"impossible count=${n}").left
      case 0 => LazyList.empty.right
      case n if n > 0 =>
        def k(i: Int): ChunkKey = ChunkKey(path, i)
        LazyList.range(1, n+1).map(i => dba.get(k(i)).flatMap(_.cata(_.right, Fail(s"chunk=${i} is not exists").left))).right
    }
  }

  def delete(path: PathKey)(implicit dba: Dba): Res[File] = {
    for {
      file <- get(path)
      _ <- LazyList.range(1, file.count+1).map(i => dba.delete(ChunkKey(path, i))).sequence_
      _ <- dba.delete(path)
    } yield file
  }

  def copy(fromPath: PathKey, toPath: PathKey)(implicit dba: Dba): Res[File] = {
    for {
      from <- get(fromPath)
      _ <- get(toPath).fold(
        l => l match {
          case _: FileNotExists => ().right
          case _ => l.left
        },
        _ => FileAlreadyExists(toPath).left
      )
      _ <- LazyList.range(1, from.count+1).map(i => for {
        x <- {
          val k = ChunkKey(fromPath, i)
          dba.get(k).flatMap(_.cata(_.right, Fail(s"chunk=${i} is not exists").left))
        }
        _ <- dba.put(ChunkKey(toPath, i), x)
      } yield ()).sequence_
      to = File(from.count, from.size, from.dir)
      _ <- dba.put(toPath, pickle(to))
    } yield to
  }
}
