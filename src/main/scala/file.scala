package mws.kvs
package file

import mws.kvs.store.Dba
import scalaz._
import scalaz.Scalaz._

/**
 * id – filepath or whatever uniquely identifies this file
 * count – number of chunks
 * size - size of file in bytes
 */
final case class File(name: String, count: Int, size: Long)

trait FileHandler {
  protected val chunkLength: Int

  protected def pickle(e: File): Array[Byte]
  protected def unpickle(a: Array[Byte]): Res[File]

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
        dba.put(s"${dir}/${name}", pickle(f)).map(_ => f)
      case _ => l.left
    },
    r => FileAlreadyExists(dir, name).left
  )

  def append(dir: String, name: String, chunk: Array[Byte])(implicit dba: Dba): Res[File] = {
    for {
      file <- get(dir, name)
      chunk1 = chunk.grouped(chunkLength).zipWithIndex
      _ <- chunk1.toStream.map{ case (x, y) => dba.put(s"${dir}/${name}_chunk_${file.count+y+1}", x) }.sequence_
      file1 = file.copy(count = file.count + chunk1.length, size = file.size + chunk.length)
      file2 = pickle(file1)
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
      _ <- dba.put(s"${dir}/${toName}", pickle(to))
    } yield to
  }
}
