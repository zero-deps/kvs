package mws.kvs

import akka.actor._
import akka.testkit._
import mws.kvs.file._
import org.scalatest._
import scala.annotation.tailrec
import scalaz._
import scalaz.Scalaz._

class FileHandlerTest extends TestKit(ActorSystem("Test"))
  with FreeSpecLike with Matchers with EitherValues with BeforeAndAfterAll {

  val kvs = Kvs(system)

  Thread.sleep(2000)

  override def afterAll = TestKit.shutdownActorSystem(system)

  val dir = "dir"
  val name = "name" + java.util.UUID.randomUUID.toString

  implicit val fh: FileHandler = new FileHandler {
    override val chunkLength = 5

    import scala.pickling._, Defaults._, binary._
    override def pickle(e: File): Res[Array[Byte]] = e.pickle.value.right
    override def unpickle(a: Array[Byte]): Res[File] = a.unpickle[File].right
  }

  "file" - {
    "create" in {
      kvs.file.create(dir, name).isRight should be (true)
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
    "content" in {
      val r = kvs.file.stream(dir, name)
      r.isRight should be (true)
      val r1 = r.toEither.right.value.sequenceURun
      r1.isRight should be (true)
      r1.toEither.right.value.toArray.flatten should be (Array(1, 2, 3, 4, 5, 6))
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
