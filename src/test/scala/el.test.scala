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
  Await.ready(kvs.onReady{},Duration("1 min"))

  override def afterAll = TestKit.shutdownActorSystem(system)

  "el handler should" - {
    import Handler._
    "return error when element is absent" in {
      kvs.get("key") should be ('left)
    }
    "save value" in {
      kvs.put("key","value").right.value should be ("value")
    }
    "retrieve value" in {
      kvs.get("key").right.value should be ("value")
    }
    "override value" in {
      kvs.put("key","value2").right.value should be ("value2")
    }
    "delete value" in {
      kvs.delete("key").right.value should be ("value2")
    }
    "clean up" in {
      kvs.get("key") should be ('left)
    }
  }
}
