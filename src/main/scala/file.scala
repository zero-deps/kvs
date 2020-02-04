package zd.kvs
package file

import zd.kvs.store.Dba
import scala.annotation.tailrec
import zd.proto.api.{N, MessageCodec}
import zd.proto.macrosapi.caseCodecAuto
import zd.gs.z._
import zd.proto.Bytes

final case class Path(@N(1) dir: Bytes, @N(2) name: Bytes)
final case class Chunk(@N(1) dir: Bytes, @N(2) name: Bytes, @N(3) num: Int)
final case class File
  ( // name – unique value inside directory
    @N(1) name: Bytes
    // count – number of chunks
  , @N(2) count: Int
    // size - size of file in bytes
  , @N(3) size: Long
    // true if directory
  , @N(4) dir: Boolean
  )

trait FileHandler {
  protected val chunkLength: Int

  private implicit val fileCodec: MessageCodec[File] = caseCodecAuto[File]
  private implicit val pathCodec: MessageCodec[Path] = caseCodecAuto[Path]
  private implicit val chunkCodec: MessageCodec[Chunk] = caseCodecAuto[Chunk]

  private def get(dir: Bytes, name: Bytes)(implicit dba: Dba): Res[File] = {
    dba.get(pickle(Path(dir=dir, name=name))) match {
      case Right(Some(x)) => unpickle[File](x).right
      case Right(None) => FileNotExists(dir, name).left
      case x@Left(_) => x.coerceRight
    }
  }

  def create(dir: Bytes, name: Bytes)(implicit dba: Dba): Res[File] = {
    val path = pickle(Path(dir=dir, name=name))
    dba.get(path) match {
      case Right(Some(_)) => FileAlreadyExists(dir, name).left
      case Right(None) =>
        val f = File(name, count=0, size=0L, dir=false)
        for {
          _ <- dba.put(path, pickle(f))
        } yield f
      case x@Left(_) => x.coerceRight
    }
  }

  def append(dir: Bytes, name: Bytes, data: Bytes)(implicit dba: Dba): Res[File] = {
    @tailrec
    def writeChunks(count: Int, rem: Bytes): Res[Int] = {
      rem.splitAt(chunkLength) match {
        case (xs, _) if xs.length == 0 => count.right
        case (xs, ys) =>
          dba.put(pickle(Chunk(dir=dir, name=name, num=count+1)), xs) match {
            case r @ Right(_) => writeChunks(count+1, rem=ys)
            case l @ Left(_) => l.coerceRight
          }
      }
    }
    for {
      _ <- (data.length == 0).fold(Fail("data is empty").left, ().right)
      file <- get(dir, name)
      count <- writeChunks(file.count, rem=data)
      file1 = file.copy(count=count, size=file.size+data.length)
      _ <- dba.put(pickle(Path(dir=dir, name=name)), pickle(file1))
    } yield file1
  }

  def size(dir: Bytes, name: Bytes)(implicit dba: Dba): Res[Long] = {
    get(dir, name).map(_.size)
  }

  def stream(dir: Bytes, name: Bytes)(implicit dba: Dba): Res[LazyList[Res[Bytes]]] = {
    get(dir, name).map(_.count).flatMap{
      case n if n < 0 => Fail(s"impossible count=${n}").left
      case 0 => LazyList.empty.right
      case n if n > 0 =>
        def k(i: Int): Bytes = pickle(Chunk(dir=dir, name=name, num=i))
        LazyList.range(1, n+1).map(i => dba.get(k(i)).flatMap(_.cata(_.right, Fail(s"chunk=${i} is not exists").left))).right
    }
  }

  def delete(dir: Bytes, name: Bytes)(implicit dba: Dba): Res[File] = {
    for {
      file <- get(dir, name)
      _ <- LazyList.range(1, file.count+1).map(i => dba.delete(pickle(Chunk(dir=dir, name=name, num=i)))).sequence_
      _ <- dba.delete(pickle(Path(dir=dir, name=name)))
    } yield file
  }

  def copy(dir: Bytes, name: (Bytes, Bytes))(implicit dba: Dba): Res[File] = {
    val (fromName, toName) = name
    for {
      from <- get(dir, fromName)
      _ <- get(dir, toName).fold(
        l => l match {
          case _: FileNotExists => ().right
          case _ => l.left
        },
        _ => FileAlreadyExists(dir, toName).left
      )
      _ <- LazyList.range(1, from.count+1).map(i => for {
        x <- {
          val k = pickle(Chunk(dir=dir, name=fromName, num=i))
          dba.get(k).flatMap(_.cata(_.right, Fail(s"chunk=${i} is not exists").left))
        }
        _ <- dba.put(pickle(Chunk(dir=dir, name=toName, num=i)), x)
      } yield ()).sequence_
      to = File(toName, from.count, from.size, from.dir)
      _ <- dba.put(pickle(Path(dir=dir, name=toName)), pickle(to))
    } yield to
  }
}
