package zd.kvs

import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{Paths, StandardOpenOption, SimpleFileVisitor, FileVisitResult, Files as JFiles, Path as JPath}
import java.io.IOException

import zio.*
import zio.blocking.*
import zio.nio.core.file.*
import zio.nio.file.*
import zio.stream.*

class FileFs(val path: String):

  private val ROOT = Path(path)

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
    val p = Paths.get(path, dir*).nn.resolve(name).nn
    ZStream.fromFile(p)

  def bytes(dir: List[String], name: String): ZIO[Blocking, IOException, Chunk[Byte]] =
    Files.readAllBytes(ROOT / dir / name)

  def delete(dir: List[String], name: String): ZIO[Blocking, Exception, Unit] =
    val p = Paths.get(path, dir*).nn.resolve(name).nn
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


  extension (p: Path)
    def / (xs: List[String]): Path = xs.foldLeft(p)((acc, x) => acc / x)

