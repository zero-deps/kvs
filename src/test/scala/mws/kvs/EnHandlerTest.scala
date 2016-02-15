package mws.kvs
package handle

import akka.actor.ActorSystem
import com.typesafe.config._
import mws.kvs.store.Memory
import org.scalatest._, matchers._, concurrent._, ScalaFutures._
import akka.testkit._, TestEvent._
import org.scalactic._

object EnHandlerTest {
  val configString = """
     kvs {
		  store="mws.kvs.store.Memory"
		 }
    """

  val config = ConfigFactory.parseString(configString)
  val FID = "testfeed"
  type EnType = En[String]
}

class EnHandlerTest extends TestKit(ActorSystem("Test", EnHandlerTest.config)) with FreeSpecLike with Matchers with EitherValues {
  import EnHandlerTest._
  import Handler._

  val kvs = Kvs(system)
  val entry1 = En[String](FID, "111111", data = "value1")
  val entry2 = En[String](FID, "111112", data = "value2")

  "Feed should" - {
    "be empty at creation" in {
      kvs.entries[EnType](FID).left.get.name shouldBe "error"
    }

    "should save value1" in {
      val saved = kvs.add(entry1).right.get
      (saved.fid, saved.id, saved.data) shouldBe (entry1.fid, entry1.id, entry1.data)
    }

    "should save value2" in {
      val saved = kvs.add(entry2).right.get
      (saved.fid, saved.id, saved.data) shouldBe (entry2.fid, entry2.id, entry2.data)
      println(kvs.dba.asInstanceOf[Memory].storage)
    }

    "should get two values from feed" in {
      val entries = kvs.entries[EnType](FID)

      entries.right.get.size shouldBe 2
      entries.right.get(0).fid shouldBe entry2.fid
      entries.right.get(0).data shouldBe entry2.data
      entries.right.get(0).id shouldBe entry2.id

      entries.right.get(1).fid shouldBe entry1.fid
      entries.right.get(1).data shouldBe entry1.data
      entries.right.get(1).id shouldBe entry1.id

    }

  }
}