package zd.kvs

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.*

class RksTest extends TestKit(ActorSystem("test")), AnyFreeSpecLike, Matchers, EitherValues, BeforeAndAfterAll {
  val kvs = Kvs.rks(system, "target/rkstest")

  "return error when element is absent" in {
    kvs.el.get[String]("key").right.value shouldBe None
  }
  "save value" in {
    kvs.el.put("key", "value").right.value shouldBe "value"
  }
  "retrieve value" in {
    kvs.el.get[String]("key").right.value shouldBe Some("value")
  }
  "override value" in {
    kvs.el.put("key", "value2").right.value shouldBe "value2"
  }
  "delete value" in {
    kvs.el.delete[String]("key").right.value shouldBe ()
  }
  "clean up" in {
    kvs.el.get[String]("key").right.value shouldBe None
    kvs.close()
  }

  override def afterAll() = {
    TestKit.shutdownActorSystem(system)
  }
}
