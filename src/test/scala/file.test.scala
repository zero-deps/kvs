package mws.kvs

import akka.actor._
import akka.testkit._
import com.typesafe.config.{ConfigFactory}
import mws.kvs.file._
import org.scalatest._
import scala.annotation.tailrec
import scalaz._

class FileHandlerTest extends TestKit(ActorSystem("Test", ConfigFactory.parseString(conf.tmpl(port=4003))))
  with FreeSpecLike with Matchers with EitherValues with BeforeAndAfterAll {

  val kvs = Kvs(system)

  Thread.sleep(2000)

  override def afterAll = TestKit.shutdownActorSystem(system)

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
      kvs.file.create(dir, name).toEither.left.value should be (FileAlreadyExists(dir, name))
    }
    "append" in {
      val r = kvs.file.append(dir, name, Array(1, 2, 3, 4, 5, 6))
      r.isRight should be (true)
      r.toEither.right.value.size should be (6)
      r.toEither.right.value.count should be (2)
    }
    "size" in {
      val r = kvs.file.size(dir, name)
      r.isRight should be (true)
      r.toEither.right.value should be (6)
    }
    "size if absent" in {
      kvs.file.size(dir, name + "1").toEither.left.value should be (FileNotExists(dir, name + "1"))
    }
    "content" in {
      val r = kvs.file.stream(dir, name)
      r.isRight should be (true)
      val r1 = r.toEither.right.value.sequenceURun
      r1.isRight should be (true)
      r1.toEither.right.value.toArray.flatten should be (Array(1, 2, 3, 4, 5, 6))
    }
    "content if absent" in {
      kvs.file.stream(dir, name + "1").toEither.left.value should be (FileNotExists(dir, name + "1"))
    }
    "delete" in {
      kvs.file.delete(dir, name).isRight should be (true)
    }
    "delete if absent" in {
      kvs.file.delete(dir, name).toEither.left.value should be (FileNotExists(dir, name))
    }
  }

  import scala.collection.immutable.LinearSeq
  implicit class LinearSeqDisjunctionExt[A,B](xs: LinearSeq[A \/ B]) {
    @tailrec
    private def _loop(ys: LinearSeq[A \/ B], acc: Vector[B]): A \/ List[B] = {
      ys.headOption match {
        case None => \/-(acc.toList)
        case Some(-\/(z)) => -\/(z)
        case Some(\/-(z)) => _loop(ys.tail, acc :+ z)
      }
    }
    def sequenceURun: A \/ List[B] = _loop(xs, Vector.empty)
  }
}
