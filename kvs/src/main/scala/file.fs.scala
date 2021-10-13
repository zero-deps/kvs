package zd.kvs

import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{StandardOpenOption, SimpleFileVisitor, FileVisitResult, Files as JFiles, Path as JPath}
import java.io.IOException

import zio.*
import zio.blocking.*
import zio.nio.core.file.*
import zio.nio.file.*
import zio.stream.*

class FileFs(val root: JPath):

  private val ROOT = Path.fromJava(root)

  def create(path: List[String]): ZIO[Blocking, Exception, Unit] =
    for
      _ <- Files.createDirectories(ROOT / path.init)
      _ <- Files.createFile(ROOT / path)
    yield unit

  def createDir(path: List[String]): ZIO[Blocking, Exception, Unit] =
    Files.createDirectories(ROOT / path)

  def append(path: List[String], data: Chunk[Byte]): ZIO[Blocking, Exception, Unit] =
   Files.writeBytes(ROOT / path, data, StandardOpenOption.APPEND) 

  def size(path: List[String]): ZIO[Blocking, IOException, Long] =
    Files.size(ROOT / path)

  def stream(path: List[String]): ZStream[Blocking, Throwable, Byte] =
    val p = root.resolve(path.mkString("/")).nn
    ZStream.fromFile(p)

  def bytes(path: List[String]): ZIO[Blocking, IOException, Chunk[Byte]] =
    Files.readAllBytes(ROOT / path)

  def delete(path: List[String]): ZIO[Blocking, Exception, Unit] =
    val p = root.resolve(path.mkString("/")).nn
    effectBlocking(JFiles.walkFileTree(p, new SimpleFileVisitor[JPath]() {
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

  def move(pathFrom: List[String], pathTo: List[String]): ZIO[Blocking, Exception, Unit] =
    Files.move(ROOT / pathFrom, ROOT / pathTo)

  extension (p: Path)
    def / (xs: List[String]): Path = xs.foldLeft(p)((acc, x) => acc / x)

