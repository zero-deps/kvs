package kvs.search

import kvs.rng.AckReceiverErr
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
  case object FileAlreadyExists
  case object FileNotExists
  case object EmptyData
  case object KeyNotFound
  type Err = FileAlreadyExists.type | FileNotExists.type | Throwable | AckReceiverErr | EmptyData.type | KeyNotFound.type

  private val chunkLength: Int = 10_000_000 // 10 MB

  given MessageCodec[File] = caseCodecAuto

  private inline def pickle(e: File): Array[Byte] = encode(e)
  
  private def unpickle(a: Array[Byte]): Either[Err, File] = Try(decode[File](a)) match
    case Success(x) => Right(x)
    case Failure(x) => Left(x)

  private def get(dir: String, name: String)(using dba: DbaEff): Either[Err, File] =
    val path = s"/search/file/$dir/$name"
    dba.get(path) match
      case Right(Some(x)) => unpickle(x)
      case Right(None) => Left(FileNotExists)
      case Left(e) => Left(e)

  def create(dir: String, name: String)(using dba: DbaEff): Either[Err, File] =
    dba.get(s"/search/file/$dir/${name}") match
      case Right(Some(_)) => Left(FileAlreadyExists)
      case Right(None) =>
        val f = File(name, count=0, size=0L)
        val x = pickle(f)
        for
          _ <- dba.put(s"/search/file/$dir/${name}", x)
        yield f
      case Left(e) => Left(e)

  def append(dir: String, name: String, data: Array[Byte])(using dba: DbaEff): Either[Err, File] =
    append(dir, name, data, data.length)

  def append(dir: String, name: String, data: Array[Byte], length: Int)(using dba: DbaEff): Either[Err, File] =
    @tailrec 
    def writeChunks(count: Int, rem: Array[Byte]): Either[Err, Int] =
      rem.splitAt(chunkLength) match
        case (xs, _) if xs.length == 0 => Right(count)
        case (xs, ys) =>
          dba.put(s"/search/file/$dir/${name}_chunk_${count+1}", xs) match
            case Right(_) => writeChunks(count+1, rem=ys)
            case Left(e) => Left(e)
    for
      _ <- if length == 0 then Left(EmptyData) else Right(())
      file <- get(dir, name)
      count <- writeChunks(file.count, rem=data)
      file1 = file.copy(count=count, size=file.size+length)
      file2 = pickle(file1)
      _ <- dba.put(s"/search/file/$dir/${name}", file2)
    yield file1

  def size(dir: String, name: String)(using DbaEff): Either[Err, Long] =
    get(dir, name).map(_.size)

  def stream(dir: String, name: String)(using dba: DbaEff): Either[Err, ArrayList[ByteBuffer]] =
    for
      file <- get(dir, name)
      xs <-
        file.count match
          case 0 => Right(ArrayList(0))
          case n =>
            inline def k(i: Int) = s"/search/file/$dir/${name}_chunk_${i}"
            @tailrec
            def loop(i: Int, acc: ArrayList[ByteBuffer]): Either[Err, ArrayList[ByteBuffer]] =
              if i <= n then
                (dba.get(k(i)).flatMap(_.fold(Left(KeyNotFound))(x => Right(ByteBuffer.wrap(x)))): Either[Err, ByteBuffer]) match
                  case Right(x) =>
                    acc.add(x)
                    loop(i + 1, acc)
                  case Left(e) => Left(e)
              else
                Right(acc)
            loop(1, ArrayList[ByteBuffer](n))
    yield xs

  def delete(dir: String, name: String)(using dba: DbaEff): Either[Err, File] =
    for
      file <- get(dir, name)
      _ <- LazyList.range(1, file.count+1).map(i => dba.delete(s"/search/file/$dir/${name}_chunk_${i}")).sequence_
      _ <- dba.delete(s"/search/file/$dir/${name}")
    yield file

  def copy(dir: String, name: (String, String))(using dba: DbaEff): Either[Err, File] =
    val (fromName, toName) = name
    for
      from <- get(dir, fromName)
      _ <- (get(dir, toName).fold(
        l => l match {
          case FileNotExists => Right(())
          case _ => Left(l)
        },
        _ => Left(FileAlreadyExists)
      ): Either[Err, Unit])
      _ <-
        (LazyList.range(1, from.count+1).map(i =>
          ((for
            x <- ({
              val k = s"/search/file/$dir/${fromName}_chunk_${i}"
              dba.get(k).flatMap(_.fold(Left(KeyNotFound))(Right(_)))
            }: Either[Err, Array[Byte]])
            _ <- dba.put(s"/search/file/$dir/${toName}_chunk_${i}", x)
          yield ()): Either[Err, Unit])
        ).sequence_ : Either[Err, Unit])
      to = File(toName, from.count, from.size)
      x = pickle(to)
      _ <- dba.put(s"/search/file/$dir/${toName}", x)
    yield to

  extension [A, B](xs: Seq[Either[A, B]])
    @tailrec
    private def _sequence_(ys: Seq[Either[A, B]]): Either[A, Unit] =
      ys.headOption match
        case None => Right(())
        case Some(Left(e)) => Left(e)
        case Some(Right(z)) => _sequence_(ys.tail)
    def sequence_ : Either[A, Unit] = _sequence_(xs)

end File
