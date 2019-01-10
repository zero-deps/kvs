package mws.kvs
package file

import mws.kvs.store.Dba
import scalaz._
import scalaz.Scalaz._
import scala.annotation.tailrec

trait FileHandler {
  protected val chunkLength: Int

  private def pickle(e: File): Res[Array[Byte]] = e.toByteArray.right
  private def unpickle(a: Array[Byte]): Res[File] = File.parseFrom(a).right

  private def get(dir: String, name: String)(implicit dba: Dba): Res[File] = dba.get(s"${dir}/${name}").fold(
    l => l match {
      case NotFound(_) => FileNotExists(dir, name).left
      case _ => l.left
    },
    r => r.right
  ).flatMap(unpickle)

  def create(dir: String, name: String)(implicit dba: Dba): Res[File] = dba.get(s"${dir}/${name}").fold(
    l => l match {
      case _: NotFound =>
        val f = File(name, count=0, size=0L)
        for {
          x <- pickle(f)
          r <- dba.put(s"${dir}/${name}", x).map(_ => f)
        } yield r
      case _ => l.left
    },
    r => FileAlreadyExists(dir, name).left
  )

  def append(dir: String, name: String, data: Array[Byte])(implicit dba: Dba): Res[File] = {
    @tailrec
    def writeChunks(count: Int, rem: Array[Byte]): Res[Int] = {
      rem.splitAt(chunkLength) match {
        case (xs, _) if xs.length === 0 => count.right
        case (xs, ys) =>
          dba.put(s"${dir}/${name}_chunk_${count+1}", xs) match {
            case r @ \/-(_) => writeChunks(count+1, rem=ys)
            case l @ -\/(_) => l
          }
      }
    }
    for {
      _ <- (data.length === 0).fold(InvalidArgument("data is empty").left, ().right)
      file <- get(dir, name)
      count <- writeChunks(file.count, rem=data)
      file1 = file.copy(count=count, size=file.size+data.length)
      file2 <- pickle(file1)
      _ <- dba.put(s"${dir}/${name}", file2)
    } yield file1
  }

  def size(dir: String, name: String)(implicit dba: Dba): Res[Long] = {
    get(dir, name).map(_.size)
  }

  def stream(dir: String, name: String)(implicit dba: Dba): Res[Stream[Res[Array[Byte]]]] = {
    get(dir, name).map(_.count).flatMap{
      case n if n < 0 => Fail(s"impossible count=${n}").left
      case 0 => Stream.empty.right
      case n if n > 0 => Stream.range(1, n+1).map(i => dba.get(s"${dir}/${name}_chunk_${i}")).right
    }
  }

  def delete(dir: String, name: String)(implicit dba: Dba): Res[File] = {
    for {
      file <- get(dir, name)
      _ <- Stream.range(1, file.count+1).map(i => dba.delete(s"${dir}/${name}_chunk_${i}")).sequence_
      _ <- dba.delete(s"${dir}/${name}")
    } yield file
  }

  def copy(dir: String, name: (String, String))(implicit dba: Dba): Res[File] = {
    val (fromName, toName) = name
    for {
      from <- get(dir, fromName)
      _ <- get(dir, toName).fold(
        l => l match {
          case _: FileNotExists => ().right
          case _ => l.left
        },
        r => FileAlreadyExists(dir, toName).left
      )
      _ <- Stream.range(1, from.count+1).map(i => for {
        x <- dba.get(s"${dir}/${fromName}_chunk_${i}")
        _ <- dba.put(s"${dir}/${toName}_chunk_${i}", x)
      } yield ()).sequence_
      to = File(toName, from.count, from.size)
      x <- pickle(to)
      _ <- dba.put(s"${dir}/${toName}", x)
    } yield to
  }
}
