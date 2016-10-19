package mws.kvs
package handle

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import akka.actor.ActorSystem
import akka.testkit._
import org.scalatest._

object EnHandlerTest {
  val fid = "fid"
  type EnType = En[FeedEntry]

  final case class FeedEntry(string:String,twoDimVector:Vector[Vector[(String,String)]],anotherVector:Vector[String])

  implicit object FeedEntryEnHandler extends EnHandler[FeedEntry] {
    import scala.pickling._,Defaults._,binary._
    def pickle(e: En[FeedEntry]): Array[Byte] = e.pickle.value
    def unpickle(a: Array[Byte]): En[FeedEntry] = a.unpickle[En[FeedEntry]]
  }
}

class EnHandlerTest extends TestKit(ActorSystem("Test"))
  with FreeSpecLike with Matchers with EitherValues with BeforeAndAfterAll {

  import EnHandlerTest._

  val kvs = Kvs(system)
  Await.ready(kvs.onReady{},Duration("1 min"))

  val mod = 50
  def entry(n:Int):EnType = En(fid,s"$n",FeedEntry(s"string$n", Vector.fill(n % mod,n % mod)((s"string$n",s"string$n")), Vector.fill(n % mod)(s"string$n")))

  val e1 = entry(1)
  val e2 = entry(2)
  val e3 = entry(3)
  val e5 = entry(5)

  override def afterAll = TestKit.shutdownActorSystem(system)

  "Feed should" - {
    "be empty at creation" in {
      kvs.entries[EnType](fid) should be ('left)
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
      val entries = kvs.entries[EnType](fid)

      entries.right.get.size shouldBe 2

      (entries.right.get(0).fid, entries.right.get(0).id, entries.right.get(0).data) shouldBe(e1.fid, e1.id, e1.data)
      (entries.right.get(1).fid, entries.right.get(1).id, entries.right.get(1).data) shouldBe(e2.fid, e2.id, e2.data)
    }

    "should save entry(3)" in {
      val saved = kvs.add(e3).right.get
      (saved.fid, saved.id, saved.data) shouldBe(e3.fid, e3.id, e3.data)
    }

    "should not save entry(2) again" in {
      kvs.add(e2).left.value should be (s"entry 2 exist in $fid")
    }

    "should get 3 values from feed" in {
      val entries = kvs.entries[EnType](fid)

      entries.right.get.size shouldBe 3

      (entries.right.get(0).fid, entries.right.get(0).id, entries.right.get(0).data) shouldBe(e1.fid, e1.id, e1.data)
      (entries.right.get(1).fid, entries.right.get(1).id, entries.right.get(1).data) shouldBe(e2.fid, e2.id, e2.data)
      (entries.right.get(2).fid, entries.right.get(2).id, entries.right.get(2).data) shouldBe(e3.fid, e3.id, e3.data)
    }

    "should not remove unexisting entry from feed" in {
      kvs.remove(e5).left.value should be (s"not_found key ${e5.fid}.${e5.id}")
    }

    "should remove entry(2) from feed without prev/next/data" in {
      val deleted = kvs.remove(En[FeedEntry](e2.fid,e2.id)).right.get

      (deleted.fid, deleted.id, deleted.data) shouldBe(e2.fid, e2.id, e2.data)
    }

    "should get 2 values from feed" in {
      val entries = kvs.entries[EnType](fid)

      entries.right.get.size shouldBe 2

      (entries.right.get(0).fid, entries.right.get(0).id, entries.right.get(0).data) shouldBe(e1.fid, e1.id, e1.data)
      (entries.right.get(1).fid, entries.right.get(1).id, entries.right.get(1).data) shouldBe(e3.fid, e3.id, e3.data)
    }

    "should remove entry(1) from feed" in {
      val deleted = kvs.remove(e1).right.get

      (deleted.fid, deleted.id, deleted.data) shouldBe(e1.fid, e1.id, e1.data)
    }

    "should get 1 values from feed" in {
      val entries = kvs.entries[EnType](fid)

      entries.right.get.size shouldBe 1

      (entries.right.get(0).fid, entries.right.get(0).id, entries.right.get(0).data) shouldBe(e3.fid, e3.id, e3.data)
    }

    "should remove entry(3) from feed" in {
      val deleted = kvs.remove(e3).right.get

      (deleted.fid, deleted.id, deleted.data) shouldBe(e3.fid, e3.id, e3.data)
    }

    "should be empty" in {
      kvs.entries[EnType](fid).right.get shouldBe empty
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

        val entries = kvs.entries[EnType](fid)

        entries.right.get.size shouldBe (limit - n)
      }
    }

    "feed should be empty at the end test" in {
      kvs.entries[EnType](fid).right.value.length should be (0)
      import Handler._
      kvs.delete(Fd(fid))
      kvs.entries[EnType](fid) should be ('left)
    }
  }
}
