package zd.kvs

import akka.actor._
import akka.testkit._
import com.typesafe.config.{ConfigFactory}
import zd.kvs.file._
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try
import zd.gs.z._
import zd.proto.Bytes

class FileHandlerTest extends TestKit(ActorSystem("Test", ConfigFactory.parseString(conf.tmpl(port=4013))))
  with AnyFreeSpecLike with Matchers with EitherValues with BeforeAndAfterAll {

  var kvs: Kvs = null
  override def beforeAll = {
    kvs = Kvs(system)
    Try(Await.result(kvs.onReady, FiniteDuration(1, MINUTES)))
  }
  override def afterAll = TestKit.shutdownActorSystem(system)

  val dir = stob("dir")
  val name = stob("name" + java.util.UUID.randomUUID.toString)
  val name1 = stob("name" + java.util.UUID.randomUUID.toString + "1")

  implicit val fh: FileHandler = new FileHandler {
    override val chunkLength = 5
  }

  def stob(x: String): Bytes = Bytes.unsafeWrap(x.getBytes)

  "file" - {
    "create" in {
      kvs.file.create(dir, name).isRight should be (true)
    }
    "create if exists" in {
      kvs.file.create(dir, name).left.value should be (FileAlreadyExists(dir, name))
    }
    "append" in {
      val r = kvs.file.append(dir, name, Bytes.unsafeWrap(Array(1, 2, 3, 4, 5, 6)))
      r.isRight should be (true)
      r.getOrElse(???).size should be (6)
      r.getOrElse(???).count should be (2)
    }
    "size" in {
      val r = kvs.file.size(dir, name)
      r.isRight should be (true)
      r.getOrElse(???) should be (6)
    }
    "size if absent" in {
      kvs.file.size(dir, name1).left.value should be (FileNotExists(dir, name1))
    }
    "content" in {
      kvs.file.stream(dir, name) shouldBe LazyList(Bytes.unsafeWrap(Array(1, 2, 3, 4, 5)).right, Bytes.unsafeWrap(Array(6)).right).right
    }
    "content if absent" in {
      kvs.file.stream(dir, name1).left.value should be (FileNotExists(dir, name1))
    }
    "delete" in {
      kvs.file.delete(dir, name).isRight should be (true)
    }
    "delete if absent" in {
      kvs.file.delete(dir, name).left.value should be (FileNotExists(dir, name))
    }
  }
}
