package kvs

import akka.actor._
import akka.testkit._
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest._
import zero.ext._, either._, option._
import zd.proto.Bytes

class ElHandlerTest extends TestKit(ActorSystem("ElHandlerTest"))
  with AnyFreeSpecLike with Matchers with EitherValues with BeforeAndAfterAll {

  implicit val dba = store.Mem()
  val kvs = el.ElHandler
  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  def key(x: String): ElKey = ElKeyExt.from_str(x)
  def stob(x: String): Bytes = Bytes.unsafeWrap(x.getBytes)

  "el handler should" - {
    "return error when element is absent" in {
      kvs.get(key("k")) shouldBe none.right
    }
    "save value" in {
      kvs.put(key("k"), stob("v")) shouldBe ().right
    }
    "retrieve value" in {
      kvs.get(key("k")) shouldBe stob("v").some.right
    }
    "override value" in {
      kvs.put(key("k"), stob("v2")) shouldBe ().right
    }
    "delete value" in {
      kvs.delete(key("k")) shouldBe ().right
    }
    "delete value again" in {
      kvs.delete(key("k")) shouldBe ().right
    }
    "clean up" in {
      kvs.get(key("k")) shouldBe none.right
    }
  }
}
