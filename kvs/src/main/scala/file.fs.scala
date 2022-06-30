package zd.kvs

import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{StandardOpenOption, SimpleFileVisitor, FileVisitResult, Files as JFiles, Path as JPath}
import java.io.IOException

import zio.*
import zio.nio.file.*
import zio.stream.*

class FileFs(val root: JPath):

  private val ROOT = Path.fromJava(root)

  def create(path: List[String]): ZIO[Any, Exception, Unit] =
    for
      _ <- Files.createDirectories(ROOT / path.init)
      _ <- Files.createFile(ROOT / path)
    yield unit

  def createDir(path: List[String]): ZIO[Any, Exception, Unit] =
    Files.createDirectories(ROOT / path)

  def append(path: List[String], data: Chunk[Byte]): ZIO[Any, Exception, Unit] =
   Files.writeBytes(ROOT / path, data, StandardOpenOption.APPEND) 

  def size(path: List[String]): ZIO[Any, IOException, Long] =
    Files.size(ROOT / path)

  def stream(path: List[String]): ZStream[Any, Throwable, Byte] =
    val p = root.resolve(path.mkString("/")).nn
    ZStream.fromFile(p.toFile.nn)

  def bytes(path: List[String]): ZIO[Any, IOException, Chunk[Byte]] =
    Files.readAllBytes(ROOT / path)

  def delete(path: List[String]): ZIO[Any, Exception, Unit] =
    val p = root.resolve(path.mkString("/")).nn
    ZIO.attemptBlocking(JFiles.walkFileTree(p, new SimpleFileVisitor[JPath]() {
      override def visitFile(file: JPath, attrs: BasicFileAttributes): FileVisitResult = {
        JFiles.delete(file)
        FileVisitResult.CONTINUE
      }
      override def postVisitDirectory(file: JPath, e: IOException): FileVisitResult = {
        JFiles.delete(file)
        FileVisitResult.CONTINUE
      }
    })).unit
      .refineToOrDie[Exception]

  def move(pathFrom: List[String], pathTo: List[String]): ZIO[Any, Exception, Unit] =
    Files.move(ROOT / pathFrom, ROOT / pathTo)

  extension (p: Path)
    def / (xs: List[String]): Path = xs.foldLeft(p)((acc, x) => acc / x)

