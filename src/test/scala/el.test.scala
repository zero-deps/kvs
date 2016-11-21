package mws.kvs

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import org.scalatest._
import akka.actor._
import akka.testkit._
import handle._

class ElHandlerTest extends TestKit(ActorSystem("Test"))
  with FreeSpecLike with Matchers with EitherValues with BeforeAndAfterAll {

  val kvs = Kvs(system)

  Thread.sleep(2000)

  override def afterAll = TestKit.shutdownActorSystem(system)

  "el handler should" - {
    import Handler._
    "return error when element is absent" in {
      kvs.get[String]("key") should be ('left)
    }
    "save value" in {
      kvs.put("key","value").right.value should be ("value")
    }
    "retrieve value" in {
      kvs.get[String]("key").right.value should be ("value")
    }
    "override value" in {
      kvs.put("key","value2").right.value should be ("value2")
    }
    "delete value" in {
      kvs.delete[String]("key").right.value should be ("value2")
    }
    "clean up" in {
      kvs.get[String]("key") should be ('left)
    }
  }
}
