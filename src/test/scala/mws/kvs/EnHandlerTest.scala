package mws.kvs
package handle

import akka.actor.ActorSystem
import com.typesafe.config._
import mws.kvs.handle.Schema._
import org.scalatest._, matchers._, concurrent._, ScalaFutures._
import akka.testkit._, TestEvent._
import org.scalactic._

import scala.concurrent.Await

object EnHandlerTest {
  val config = ConfigFactory.load
  val FID = new scala.util.Random().nextString(6)
  type EnType = En[FeedEntry]
}

class EnHandlerTest extends TestKit(ActorSystem("Test", EnHandlerTest.config))
  with FreeSpecLike
  with Matchers
  with EitherValues
  with DefaultTimeout
  with ImplicitSender
  with BeforeAndAfterAll {

  import EnHandlerTest._
  import Handler._

  val kvs = Kvs(system)

  val mod = 50
  def entry(n: Int) = En[FeedEntry](FID, s"$n", data = FeedEntry(s"string$n", Vector.fill(n % mod,n % mod)((s"string$n",s"string$n")), Vector.fill(n % mod)(s"string$n") ))

  val e1 = entry(1)
  val e2 = entry(2)
  val e3 = entry(3)
  val e5 = entry(5)

  override def afterAll = {
    TestKit.shutdownActorSystem(system)
  }

  "Feed should" - {
    "be empty at creation" in {
      kvs.entries[EnType](FID).left.get.name shouldBe "error"
    }

    "should save e1 " in {
      val saved = kvs.add(e1).right.get
      (saved.fid, saved.id, saved.data) shouldBe(e1.fid, e1.id, e1.data)
    }

    "should save e2" in {
      val saved = kvs.add(e2).right.get
      (saved.fid, saved.id, saved.data) shouldBe(e2.fid, e2.id, e2.data)
    }

    "should get e1 and e2 from feed" in {
      val entries = kvs.entries[EnType](FID)

      entries.right.get.size shouldBe 2

      (entries.right.get(0).fid, entries.right.get(0).id, entries.right.get(0).data) shouldBe(e2.fid, e2.id, e2.data)
      (entries.right.get(1).fid, entries.right.get(1).id, entries.right.get(1).data) shouldBe(e1.fid, e1.id, e1.data)
    }

    "should save entry(3)" in {
      val saved = kvs.add(e3).right.get
      (saved.fid, saved.id, saved.data) shouldBe(e3.fid, e3.id, e3.data)
    }

    "should not save entry(2) again" in {
      val saved = kvs.add(e2).left.get
      (saved.name, saved.msg) shouldBe("error", s"entry 2 exist in $FID")
    }

    "should get 3 values from feed" in {
      val entries = kvs.entries[EnType](FID)

      entries.right.get.size shouldBe 3

      (entries.right.get(0).fid, entries.right.get(0).id, entries.right.get(0).data) shouldBe(e3.fid, e3.id, e3.data)
      (entries.right.get(1).fid, entries.right.get(1).id, entries.right.get(1).data) shouldBe(e2.fid, e2.id, e2.data)
      (entries.right.get(2).fid, entries.right.get(2).id, entries.right.get(2).data) shouldBe(e1.fid, e1.id, e1.data)
    }

    "should not remove unexisting entry from feed" in {
      val deleted = kvs.remove(e5).left.get

      (deleted.name, deleted.msg) shouldBe("error", s"not_found key ${e5.fid}.${e5.id}")  //Dbe(error,not_found key 우籁차ᮔঔ✓.5)
    }

    "should remove entry(2) from feed" in {
      val deleted = kvs.remove(e2).right.get

      (deleted.fid, deleted.id, deleted.data) shouldBe(e2.fid, e2.id, e2.data)
    }

    "should get 2 values from feed" in {
      val entries = kvs.entries[EnType](FID)

      entries.right.get.size shouldBe 2

      (entries.right.get(0).fid, entries.right.get(0).id, entries.right.get(0).data) shouldBe(e3.fid, e3.id, e3.data)
      (entries.right.get(1).fid, entries.right.get(1).id, entries.right.get(1).data) shouldBe(e1.fid, e1.id, e1.data)
    }

    "should remove entry(1) from feed" in {
      val deleted = kvs.remove(e1).right.get

      (deleted.fid, deleted.id, deleted.data) shouldBe(e1.fid, e1.id, e1.data)
    }


    "should get 1 values from feed" in {
      val entries = kvs.entries[EnType](FID)

      entries.right.get.size shouldBe 1

      (entries.right.get(0).fid, entries.right.get(0).id, entries.right.get(0).data) shouldBe(e3.fid, e3.id, e3.data)
    }

    "should remove entry(3) from feed" in {
      val deleted = kvs.remove(e3).right.get

      (deleted.fid, deleted.id, deleted.data) shouldBe(e3.fid, e3.id, e3.data)
    }

    "should be empty" in {
      kvs.entries[EnType](FID).right.get shouldBe empty
    }


    "should not create stack overflow" in {
      val limit = 100

      Stream.from(1,1).takeWhile( _.<=(limit)).foreach{ n =>
        val toadd= entry(n)
        val added = kvs.add(toadd).right.get
        (added.fid, added.id, added.data) shouldBe (toadd.fid, toadd.id, toadd.data)
      }


      Stream.from(1,1).takeWhile( _.<=(limit)).foreach{ n =>

        val toremove= entry(n)
        val removed = kvs.remove(toremove).right.get

        (removed.fid, removed.id, removed.data) shouldBe (toremove.fid, toremove.id, toremove.data)

        val entries = kvs.entries[EnType](FID)

        entries.right.get.size shouldBe (limit - n)
      }

    }
  }
}

