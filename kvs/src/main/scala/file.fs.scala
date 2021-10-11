package zd.kvs

import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{StandardOpenOption, SimpleFileVisitor, FileVisitResult, Files as JFiles, Path as JPath}
import java.io.IOException

import zio.*
import zio.blocking.*
import zio.nio.core.file.*
import zio.nio.file.*
import zio.stream.*

class FileFs(val path: JPath):

  private val ROOT = Path.fromJava(path)

  def create(dir: List[String], name: String): ZIO[Blocking, Exception, Unit] =
    for
      _ <- Files.createDirectories(ROOT / dir)
      _ <- Files.createFile(ROOT / dir / name)
    yield unit

  def append(dir: List[String], name: String, data: Chunk[Byte]): ZIO[Blocking, Exception, Unit] =
   Files.writeBytes(ROOT / dir / name, data, StandardOpenOption.APPEND) 

  def size(dir: List[String], name: String): ZIO[Blocking, IOException, Long] =
    Files.size(ROOT / dir / name)

  def stream(dir: List[String], name: String): ZStream[Blocking, Throwable, Byte] =
    val p = path.resolve(dir.mkString("/")).nn.resolve(name).nn
    ZStream.fromFile(p)

  def bytes(dir: List[String], name: String): ZIO[Blocking, IOException, Chunk[Byte]] =
    Files.readAllBytes(ROOT / dir / name)

  def delete(dir: List[String], name: String): ZIO[Blocking, Exception, Unit] =
    val p = path.resolve(dir.mkString("/")).nn.resolve(name).nn
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

  def rename(dir: List[String], nameFrom: String, nameTo: String): ZIO[Blocking, Exception, Unit] =
    Files.move(ROOT / dir / nameFrom, ROOT / dir / nameTo)

  def move(fromDir: List[String], toDir: List[String], name: String): ZIO[Blocking, Exception, Unit] =
    Files.move(ROOT / fromDir / name, ROOT / toDir / name)

  extension (p: Path)
    def / (xs: List[String]): Path = xs.foldLeft(p)((acc, x) => acc / x)

