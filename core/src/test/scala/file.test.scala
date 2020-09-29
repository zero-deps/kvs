package zd.kvs

import akka.actor._
import akka.testkit._
import zd.kvs.file._
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest._
import zero.ext._, either._
import zd.proto.Bytes

class FileHandlerTest extends TestKit(ActorSystem("FileHandlerTest"))
  with AnyFreeSpecLike with Matchers with EitherValues with BeforeAndAfterAll {

  val kvs = Kvs.mem()
  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  val dir = FdKey(stob("dir"))
  val name = stob("name" + System.currentTimeMillis)
  val path = PathKey(dir, name)
  val name1 = stob("name" + System.currentTimeMillis + "_1")
  val path1 = PathKey(dir, name1)

  implicit val fh: FileHandler = new FileHandler {
    override val chunkLength = 5
  }

  def stob(x: String): Bytes = Bytes.unsafeWrap(x.getBytes)

  "file" - {
    "create" in {
      kvs.file.create(path).isRight should be (true)
    }
    "create if exists" in {
      kvs.file.create(path).left.value should be (FileAlreadyExists(path))
    }
    "append" in {
      val r = kvs.file.append(path, Bytes.unsafeWrap(Array(1, 2, 3, 4, 5, 6)))
      r.isRight should be (true)
      r.getOrElse(???).size should be (6)
      r.getOrElse(???).count should be (2)
    }
    "size" in {
      val r = kvs.file.size(path)
      r.isRight should be (true)
      r.getOrElse(???) should be (6)
    }
    "size if absent" in {
      kvs.file.size(path1).left.value should be (FileNotExists(path1))
    }
    "content" in {
      kvs.file.stream(path) shouldBe LazyList(Bytes.unsafeWrap(Array(1, 2, 3, 4, 5)).right, Bytes.unsafeWrap(Array(6)).right).right
    }
    "content if absent" in {
      kvs.file.stream(path1).left.value should be (FileNotExists(path1))
    }
    "delete" in {
      kvs.file.delete(path).isRight should be (true)
    }
    "delete if absent" in {
      kvs.file.delete(path).left.value should be (FileNotExists(path))
    }
  }
}
