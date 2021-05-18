package zd.kvs
package file

import zd.kvs.store.Dba
import scala.annotation.tailrec
import scala.util.{Try, Success, Failure}
import proto.*

trait FileHandler {
  protected val chunkLength: Int

  private implicit val fileCodec: MessageCodec[File] = caseCodecAuto[File]

  private def pickle(e: File): Res[Array[Byte]] = Right(encode(e))
  private def unpickle(a: Array[Byte]): Res[File] = Try(decode[File](a)) match {
    case Success(x) => Right(x)
    case Failure(x) => Left(UnpickleFail(x))
  }

  private def get(dir: String, name: String)(implicit dba: Dba): Res[File] = {
    dba.get(s"${dir}/${name}") match {
      case Right(Some(x)) => unpickle(x)
      case Right(None) => Left(FileNotExists(dir, name))
      case x@Left(_) => x.coerceRight
    }
  }

  def create(dir: String, name: String)(implicit dba: Dba): Res[File] = {
    dba.get(s"${dir}/${name}") match {
      case Right(Some(_)) => Left(FileAlreadyExists(dir, name))
      case Right(None) =>
        val f = File(name, count=0, size=0L, dir=false)
        for {
          x <- pickle(f)
          _ <- dba.put(s"${dir}/${name}", x)
        } yield f
      case x@Left(_) => x.coerceRight
    }
  }

  def append(dir: String, name: String, data: Array[Byte])(implicit dba: Dba): Res[File] = {
    append(dir, name, data, data.length)
  }
  def append(dir: String, name: String, data: Array[Byte], length: Int)(implicit dba: Dba): Res[File] = {
    @tailrec
    def writeChunks(count: Int, rem: Array[Byte]): Res[Int] = {
      rem.splitAt(chunkLength) match {
        case (xs, _) if xs.length == 0 => Right(count)
        case (xs, ys) =>
          dba.put(s"${dir}/${name}_chunk_${count+1}", xs) match {
            case r @ Right(_) => writeChunks(count+1, rem=ys)
            case l @ Left(_) => l.coerceRight
          }
      }
    }
    for {
      _ <- (length == 0).fold(Left(InvalidArgument("data is empty")), Right(()))
      file <- get(dir, name)
      count <- writeChunks(file.count, rem=data)
      file1 = file.copy(count=count, size=file.size+length)
      file2 <- pickle(file1)
      _ <- dba.put(s"${dir}/${name}", file2)
    } yield file1
  }

  def size(dir: String, name: String)(implicit dba: Dba): Res[Long] = {
    get(dir, name).map(_.size)
  }

  def stream(dir: String, name: String)(implicit dba: Dba): Res[LazyList[Res[Array[Byte]]]] = {
    get(dir, name).map(_.count).flatMap{
      case n if n < 0 => Left(Fail(s"impossible count=${n}"))
      case 0 => Right(LazyList.empty)
      case n if n > 0 =>
        def k(i: Int) = s"${dir}/${name}_chunk_${i}"
        Right(LazyList.range(1, n+1).map(i => dba.get(k(i)).flatMap(_.cata(Right(_), Left(NotFound(k(i)))))))
    }
  }

  def delete(dir: String, name: String)(implicit dba: Dba): Res[File] = {
    for {
      file <- get(dir, name)
      _ <- LazyList.range(1, file.count+1).map(i => dba.delete(s"${dir}/${name}_chunk_${i}")).sequence_
      _ <- dba.delete(s"${dir}/${name}")
    } yield file
  }

  def copy(dir: String, name: (String, String))(implicit dba: Dba): Res[File] = {
    val (fromName, toName) = name
    for {
      from <- get(dir, fromName)
      _ <- get(dir, toName).fold(
        l => l match {
          case _: FileNotExists => Right(())
          case _ => Left(l)
        },
        _ => Left(FileAlreadyExists(dir, toName))
      )
      _ <- LazyList.range(1, from.count+1).map(i => for {
        x <- {
          val k = s"${dir}/${fromName}_chunk_${i}"
          dba.get(k).flatMap(_.cata(Right(_), Left(NotFound(k))))
        }
        _ <- dba.put(s"${dir}/${toName}_chunk_${i}", x)
      } yield ()).sequence_
      to = File(toName, from.count, from.size, from.dir)
      x <- pickle(to)
      _ <- dba.put(s"${dir}/${toName}", x)
    } yield to
  }
}