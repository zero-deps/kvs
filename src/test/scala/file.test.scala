package kvs

import akka.actor._
import akka.testkit._
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest._
import zero.ext._, either._
import zd.proto.Bytes

import file._

class FileHandlerTest extends TestKit(ActorSystem("FileHandlerTest"))
  with AnyFreeSpecLike with Matchers with EitherValues with BeforeAndAfterAll {

  implicit val dba = store.Mem()
  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  val dir = FdKey(stob("dir"))
  val name = ElKeyExt.from_str("name" + System.currentTimeMillis)
  val path = PathKey(dir, name)
  val name1 = ElKeyExt.from_str("name" + System.currentTimeMillis + "_1")
  val path1 = PathKey(dir, name1)

  implicit val flh: FileHandler = new FileHandler {
    override val chunkLength = 5
  }

  def stob(x: String): Bytes = Bytes.unsafeWrap(x.getBytes)

  "file" - {
    "create" in {
      flh.create(path).isRight should be (true)
    }
    "create if exists" in {
      flh.create(path).left.value should be (FileAlreadyExists(path))
    }
    "append" in {
      val r = flh.append(path, Bytes.unsafeWrap(Array(1, 2, 3, 4, 5, 6)))
      r.isRight should be (true)
      r.getOrElse(???).size should be (6)
      r.getOrElse(???).count should be (2)
    }
    "size" in {
      val r = flh.size(path)
      r.isRight should be (true)
      r.getOrElse(???) should be (6)
    }
    "size if absent" in {
      flh.size(path1).left.value should be (FileNotExists(path1))
    }
    "content" in {
      flh.stream(path) shouldBe LazyList(Bytes.unsafeWrap(Array(1, 2, 3, 4, 5)).right, Bytes.unsafeWrap(Array(6)).right).right
    }
    "content if absent" in {
      flh.stream(path1).left.value should be (FileNotExists(path1))
    }
    "delete" in {
      flh.delete(path).isRight should be (true)
    }
    "delete if absent" in {
      flh.delete(path).left.value should be (FileNotExists(path))
    }
  }
}
