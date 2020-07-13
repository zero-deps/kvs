package zd.kvs

import akka.actor._
import akka.testkit._
import com.typesafe.config.{ConfigFactory}
import zd.kvs.file._
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try
import zero.ext._, either._, traverse._

class FileHandlerTest extends TestKit(ActorSystem("Test", ConfigFactory.parseString(conf.tmpl(port=4013))))
  with AnyFreeSpecLike with Matchers with EitherValues with BeforeAndAfterAll {

  var kvs: Kvs = null
  override def beforeAll(): Unit = {
    kvs = Kvs(system)
    Try(Await.result(kvs.onReady, FiniteDuration(1, MINUTES)))
  }
  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  val dir = "dir"
  val name = "name" + java.util.UUID.randomUUID.toString

  implicit val fh: FileHandler = new FileHandler {
    override val chunkLength = 5
  }

  "file" - {
    "create" in {
      kvs.file.create(dir, name).isRight should be (true)
    }
    "create if exists" in {
      kvs.file.create(dir, name).left.value should be (FileAlreadyExists(dir, name))
    }
    "append" in {
      val r = kvs.file.append(dir, name, Array[Byte](1, 2, 3, 4, 5, 6))
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
      kvs.file.size(dir, name + "1").left.value should be (FileNotExists(dir, name + "1"))
    }
    "content" in {
      val r = kvs.file.stream(dir, name)
      r.isRight should be (true)
      val r1 = r.getOrElse(???).sequence
      r1.isRight should be (true)
      r1.getOrElse(???).toArray.flatten should be (Array[Byte](1, 2, 3, 4, 5, 6))
    }
    "content if absent" in {
      kvs.file.stream(dir, name + "1").left.value should be (FileNotExists(dir, name + "1"))
    }
    "delete" in {
      kvs.file.delete(dir, name).isRight should be (true)
    }
    "delete if absent" in {
      kvs.file.delete(dir, name).left.value should be (FileNotExists(dir, name))
    }
  }
}
