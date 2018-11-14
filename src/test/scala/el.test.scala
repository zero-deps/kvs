package mws.kvs

import org.scalatest._
import akka.actor._
import akka.testkit._

class ElHandlerTest extends TestKit(ActorSystem("Test"))
  with FreeSpecLike with Matchers with EitherValues with BeforeAndAfterAll {

  val kvs = Kvs(system)

  Thread.sleep(2000)

  override def afterAll = TestKit.shutdownActorSystem(system)

  "el handler should" - {
    "return error when element is absent" in {
      kvs.el.get[String]("key").isLeft should be (true)
    }
    "save value" in {
      kvs.el.put("key","value").toEither.right.value should be ("value")
    }
    "retrieve value" in {
      kvs.el.get[String]("key").toEither.right.value should be ("value")
    }
    "override value" in {
      kvs.el.put("key","value2").toEither.right.value should be ("value2")
    }
    "delete value" in {
      kvs.el.delete[String]("key").toEither.right.value should be ("value2")
    }
    "clean up" in {
      kvs.el.get[String]("key") should be ('left)
    }
  }
}
