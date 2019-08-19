package zd.kvs

import akka.actor._
import akka.testkit._
import com.typesafe.config.{ConfigFactory}
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

class ElHandlerTest extends TestKit(ActorSystem("Test", ConfigFactory.parseString(conf.tmpl(port=4011))))
  with AnyFreeSpecLike with Matchers with EitherValues with BeforeAndAfterAll {

  var kvs: Kvs = null
  override def beforeAll = {
    kvs = Kvs(system)
    Try(Await.result(kvs.onReady, FiniteDuration(1, MINUTES)))
  }
  override def afterAll = TestKit.shutdownActorSystem(system)

  "el handler should" - {
    "return error when element is absent" in {
      kvs.el.get[String]("key").isLeft should be (true)
    }
    "save value" in {
      kvs.el.put("key","value").getOrElse(???) should be ("value")
    }
    "retrieve value" in {
      kvs.el.get[String]("key").getOrElse(???) should be ("value")
    }
    "override value" in {
      kvs.el.put("key","value2").getOrElse(???) should be ("value2")
    }
    "delete value" in {
      kvs.el.delete[String]("key").getOrElse(???) should be ("value2")
    }
    "clean up" in {
      kvs.el.get[String]("key") should be (Symbol("left"))
    }
  }
}
