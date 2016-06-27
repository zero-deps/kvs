package mws.kvs
package store

import com.typesafe.config._
import org.scalatest._
import akka.actor.ActorSystem

class MemoryTest extends FreeSpecLike with Matchers with EitherValues with BeforeAndAfterAll {
  val system = ActorSystem("Test", ConfigFactory.load)

  "Memory DBA should" - {
    val kvs = Kvs(system)
    "be empty at creation" in {
      kvs.get[String]("k1").left.value should be (s"not_found key k1")
    }
    "save successfully value" in {
      kvs.put[String]("k1", "v1").right.value should be ("v1")
    }
    "retrieve saved value" in {
      kvs.get[String]("k1").right.value should be ("v1")
    }
    "replace value" in {
      kvs.put[String]("k1", "v2") should be ('right)
      kvs.get[String]("k1").right.value should be ("v2")
    }
    "delete value" in {
      kvs.delete[String]("k1").right.value should be ("v2")
    }
    "and value is unavailable" in {
      kvs.get[String]("k1") should be ('left)
    }
  }

  override def afterAll():Unit = {
    system.shutdown()
    system.awaitTermination()
  }
}
