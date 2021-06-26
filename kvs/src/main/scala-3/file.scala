package zd.kvs

import scala.annotation.tailrec
import scala.util.{Try, Success, Failure}
import proto.*

case class File
  ( @N(1) name: String // name – unique value inside directory
  , @N(2) count: Int // count – number of chunks
  , @N(3) size: Long // size - size of file in bytes
  , @N(4) dir: Boolean // true if directory
  )

trait FileHandler:
  protected val chunkLength: Int

  given MessageCodec[File] = caseCodecAuto

  private inline def pickle(e: File): Array[Byte] = encode(e)
  private def unpickle(a: Array[Byte]): Either[Err, File] = Try(decode[File](a)) match {
    case Success(x) => Right(x)
    case Failure(x) => Left(UnpickleFail(x))
  }

  private def get(dir: String, name: String)(using dba: Dba): Either[Err, File] = {
    dba.get(s"${dir}/${name}") match {
      case Right(Some(x)) => unpickle(x)
      case Right(None) => Left(FileNotExists(dir, name))
      case Left(e) => Left(e)
    }
  }

  def create(dir: String, name: String)(using dba: Dba): Either[Err, File] = {
    dba.get(s"${dir}/${name}") match {
      case Right(Some(_)) => Left(FileAlreadyExists(dir, name))
      case Right(None) =>
        val f = File(name, count=0, size=0L, dir=false)
        val x = pickle(f)
        for {
          _ <- dba.put(s"${dir}/${name}", x)
        } yield f
      case Left(e) => Left(e)
    }
  }

  def append(dir: String, name: String, data: Array[Byte])(using dba: Dba): Either[Err, File] = {
    append(dir, name, data, data.length)
  }
  def append(dir: String, name: String, data: Array[Byte], length: Int)(using dba: Dba): Either[Err, File] = {
    @tailrec
    def writeChunks(count: Int, rem: Array[Byte]): Either[Err, Int] = {
      rem.splitAt(chunkLength) match {
        case (xs, _) if xs.length == 0 => Right(count)
        case (xs, ys) =>
          dba.put(s"${dir}/${name}_chunk_${count+1}", xs) match {
            case Right(_) => writeChunks(count+1, rem=ys)
            case Left(e) => Left(e)
          }
      }
    }
    for {
      _ <- (length == 0).fold(Left(InvalidArgument("data is empty")), Right(()))
      file <- get(dir, name)
      count <- writeChunks(file.count, rem=data)
      file1 = file.copy(count=count, size=file.size+length)
      file2 = pickle(file1)
      _ <- dba.put(s"${dir}/${name}", file2)
    } yield file1
  }

  def size(dir: String, name: String)(using Dba): Either[Err, Long] = {
    get(dir, name).map(_.size)
  }

  def stream(dir: String, name: String)(using dba: Dba): Either[Err, LazyList[Either[Err, Array[Byte]]]] = {
    get(dir, name).map(_.count).flatMap{
      case n if n < 0 => Left(Fail(s"impossible count=${n}"))
      case 0 => Right(LazyList.empty)
      case n if n > 0 =>
        def k(i: Int) = s"${dir}/${name}_chunk_${i}"
        Right(LazyList.range(1, n+1).map(i => dba.get(k(i)).flatMap(_.cata(Right(_), Left(KeyNotFound)))))
    }
  }

  def delete(dir: String, name: String)(using dba: Dba): Either[Err, File] = {
    for {
      file <- get(dir, name)
      _ <- LazyList.range(1, file.count+1).map(i => dba.delete(s"${dir}/${name}_chunk_${i}")).sequence_
      _ <- dba.delete(s"${dir}/${name}")
    } yield file
  }

  def copy(dir: String, name: (String, String))(using dba: Dba): Either[Err, File] = {
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
          dba.get(k).flatMap(_.cata(Right(_), Left(KeyNotFound)))
        }
        _ <- dba.put(s"${dir}/${toName}_chunk_${i}", x)
      } yield ()).sequence_
      to = File(toName, from.count, from.size, from.dir)
      x = pickle(to)
      _ <- dba.put(s"${dir}/${toName}", x)
    } yield to
  }
