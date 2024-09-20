package kvs.search

import proto.*
import scala.annotation.tailrec
import scala.util.{Try, Success, Failure}
import java.nio.ByteBuffer
import java.util.ArrayList

case class File
  ( @N(1) name: String // name – unique value inside directory
  , @N(2) count: Int // count – number of chunks
  , @N(3) size: Long // size - size of file in bytes
  )

object File:
  case object NoData

  private val chunkLength: Int = 10_000_000 // 10 MB

  given MessageCodec[File] = caseCodecAuto

  private inline def pickle(e: File): Array[Byte] = encode(e)
  
  private def unpickle(a: Array[Byte]): Either[Throwable, File] =
    Try(decode[File](a)) match
      case Success(x) => Right(x)
      case Failure(x) => Left(x)

  private def get(dir: String, name: String)(using dba: DbaEff)(using CanEqual[None.type, Option[dba.V]]): Either[FileNotExists.type | dba.Err, File] =
    val path = s"/search/file/$dir/$name"
    dba.get(path) match
      case Right(Some(x)) => unpickle(x)
      case Right(None) => Left(FileNotExists)
      case Left(e) => Left(e)

  def create(dir: String, name: String)(using dba: DbaEff)(using CanEqual[None.type, Option[dba.V]]): Either[dba.Err | FileExists.type, File] =
    dba.get(s"/search/file/$dir/${name}") match
      case Right(Some(_)) => Left(FileExists)
      case Right(None) =>
        val f = File(name, count=0, size=0L)
        val x = pickle(f)
        for
          _ <- dba.put(s"/search/file/$dir/${name}", x)
        yield f
      case Left(e) => Left(e)

  def append(dir: String, name: String, data: Array[Byte])(using dba: DbaEff)(using CanEqual[None.type, Option[Array[Byte]]]): Either[dba.Err | NoData.type | FileNotExists.type, File] =
    val length = data.length
    @tailrec 
    def writeChunks(count: Int, rem: Array[Byte]): Either[dba.Err, Int] =
      rem.splitAt(chunkLength) match
        case (xs, _) if xs.length == 0 => Right(count)
        case (xs, ys) =>
          dba.put(s"/search/file/$dir/${name}_chunk_${count+1}", xs) match
            case Right(_) => writeChunks(count+1, rem=ys)
            case Left(e) => Left(e)
    for
      _ <- (if length == 0 then Left(NoData) else Right(())): Either[dba.Err | NoData.type, Unit]
      file <- get(dir, name): Either[dba.Err | NoData.type | FileNotExists.type, File]
      count <- writeChunks(file.count, rem=data)
      file1 = file.copy(count=count, size=file.size+length)
      file2 = pickle(file1)
      _ <- dba.put(s"/search/file/$dir/${name}", file2)
    yield file1

  def size(dir: String, name: String)(using dba: DbaEff)(using CanEqual[None.type, Option[Array[Byte]]]): Either[dba.Err | FileNotExists.type, Long] =
    get(dir, name).map(_.size)

  def stream(dir: String, name: String)(using dba: DbaEff)(using CanEqual[None.type, Option[Array[Byte]]]): Either[dba.Err | BrokenFile.type | FileNotExists.type, ArrayList[ByteBuffer]] =
    for
      file <- (get(dir, name): Either[dba.Err | FileNotExists.type, File])
      xs <-
        (file.count match
          case 0 => Right(ArrayList(0))
          case n =>
            inline def k(i: Int) = s"/search/file/$dir/${name}_chunk_${i}"
            @tailrec
            def loop(i: Int, acc: ArrayList[ByteBuffer]): Either[dba.Err | BrokenFile.type, ArrayList[ByteBuffer]] =
              if i <= n then
                (dba.get(k(i)).flatMap(_.fold(Left(BrokenFile))(x => Right(ByteBuffer.wrap(x).nn))): Either[dba.Err | BrokenFile.type, ByteBuffer]) match
                  case Right(x) =>
                    acc.add(x)
                    loop(i + 1, acc)
                  case Left(e) => Left(e)
              else
                Right(acc)
            loop(1, ArrayList[ByteBuffer](n))): Either[dba.Err | BrokenFile.type, ArrayList[ByteBuffer]]
    yield xs

  def delete(dir: String, name: String)(using dba: DbaEff)(using CanEqual[None.type, Option[Array[Byte]]], CanEqual[None.type, Option[Either[FileNotExists.type | dba.Err, Unit]]]): Either[FileNotExists.type | dba.Err, File] =
    @tailrec
    def sequence(ys: Seq[Either[FileNotExists.type | dba.Err, Unit]]): Either[FileNotExists.type | dba.Err, Unit] =
      ys.headOption match
        case None => Right(())
        case Some(Left(e)) => Left(e)
        case Some(Right(z)) => sequence(ys.tail)
    for
      file <- get(dir, name)
      _ <- sequence(LazyList.range(1, file.count+1).map(i => dba.delete(s"/search/file/$dir/${name}_chunk_${i}")))
      _ <- dba.delete(s"/search/file/$dir/${name}")
    yield file

  def copy(
    dir: String
  , name: (String, String)
  )(using
    dba: DbaEff
  )(using
    CanEqual[FileNotExists.type, FileNotExists.type | dba.Err]
  , CanEqual[None.type, Option[Array[Byte]]]
  , CanEqual[None.type, Option[Either[dba.Err | FileNotExists.type | BrokenFile.type | FileExists.type, Unit]]]
  ): Either[dba.Err | FileNotExists.type | BrokenFile.type | FileExists.type, File] =
    val (fromName, toName) = name
    @tailrec
    def sequence(ys: Seq[Either[dba.Err | FileNotExists.type | BrokenFile.type | FileExists.type, Unit]]): Either[dba.Err | FileNotExists.type | BrokenFile.type | FileExists.type, Unit] =
      ys.headOption match
        case None => Right(())
        case Some(Left(e)) => Left(e)
        case Some(Right(z)) => sequence(ys.tail)
    for
      from <- get(dir, fromName): Either[dba.Err | FileNotExists.type | BrokenFile.type, File]
      _ <-
        get(dir, toName).fold(
          l => l match {
            case FileNotExists => Right(())
            case _ => Left(l)
          },
          _ => Left(FileExists)
        ): Either[dba.Err | FileExists.type | BrokenFile.type | FileNotExists.type, Unit]
      _ <-
        sequence(LazyList.range(1, from.count+1).map(i =>
          (for
            x <- ({
              val k = s"/search/file/$dir/${fromName}_chunk_${i}"
              dba.get(k).flatMap(_.fold(Left(BrokenFile))(Right(_)))
            }: Either[dba.Err | FileNotExists.type | BrokenFile.type | FileExists.type, dba.V])
            _ <- dba.put(s"/search/file/$dir/${toName}_chunk_${i}", x)
          yield ()): Either[dba.Err | FileNotExists.type | BrokenFile.type | FileExists.type, Unit]
        )): Either[dba.Err | FileNotExists.type | BrokenFile.type | FileExists.type, Unit]
      to = File(toName, from.count, from.size)
      x = pickle(to)
      _ <- dba.put(s"/search/file/$dir/${toName}", x)
    yield to
end File
