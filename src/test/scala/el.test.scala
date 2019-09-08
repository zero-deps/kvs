package zd.kvs

import akka.actor._
import akka.testkit._
import com.typesafe.config.{ConfigFactory}
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try
import zd.gs.z._

class ElHandlerTest extends TestKit(ActorSystem("Test", ConfigFactory.parseString(conf.tmpl(port=4011))))
  with AnyFreeSpecLike with Matchers with EitherValues with BeforeAndAfterAll {

  var kvs: Kvs = null
  override def beforeAll = {
    kvs = Kvs(system)
    Try(Await.result(kvs.onReady, FiniteDuration(1, MINUTES)))
  }
  override def afterAll = TestKit.shutdownActorSystem(system)

  def stob(x: String): Bytes = Bytes(x.getBytes)

  "el handler should" - {
    "return error when element is absent" in {
      kvs.el.get(stob("k")) shouldBe Nothing.right
    }
    "save value" in {
      kvs.el.put(stob("k"), stob("v")) shouldBe ().right
    }
    "retrieve value" in {
      kvs.el.get(stob("k")) shouldBe stob("v").just.right
    }
    "override value" in {
      kvs.el.put(stob("k"), stob("v2")) shouldBe ().right
    }
    "delete value" in {
      kvs.el.delete(stob("k")) shouldBe ().right
    }
    "delete value again" in {
      kvs.el.delete(stob("k")) shouldBe ().right
    }
    "clean up" in {
      kvs.el.get(stob("k")) shouldBe Nothing.right
    }
  }
}
