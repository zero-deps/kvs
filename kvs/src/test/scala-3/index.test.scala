package zd.kvs
package idx

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

class IdxHandlerTest extends AnyFreeSpecLike with Matchers with BeforeAndAfterAll with EitherValues {
  type Fid = IdxHandler.Fid 
  val Fid = IdxHandler.Fid 
  type Fd = IdxHandler.Fd
  val Fd = IdxHandler.Fd
  type En = IdxHandler.Idx
  val En = IdxHandler.Idx
  val fid = Fid("index")
  def entry(n: Int): En = En(fid, id=n.toString, prev=zd.kvs.empty)

  val e1 = entry(1)
  val e2 = entry(2)
  val e3 = entry(3)
  val e5 = entry(5)

  val kvs = Kvs.mem()

  "Feed should" - {
    "be empty at creation" in {
      kvs.index.all(fid) shouldBe (Right(LazyList.empty))
    }

    "should save e1" in {
      val saved = kvs.index.add(e1).getOrElse(???)
      (saved.fid, saved.id) shouldBe ((e1.fid, "1"))
    }

    "should save e2" in {
      val saved = kvs.index.add(e2).getOrElse(???)
      (saved.fid, saved.id) shouldBe ((e2.fid, "2"))
    }

    "should get e1 and e2 from feed" in {
      val stream = kvs.index.all(fid)
      stream.map(_.toList) shouldBe Right(List(Right(e2.copy(prev="1")), Right(e1)))
    }

    "should save entry(3)" in {
      val saved = kvs.index.add(e3).getOrElse(???)
      (saved.fid, saved.id) shouldBe ((e3.fid, "3"))
    }

    "should not save entry(2) again" in {
      kvs.index.add(e2).left.getOrElse(???) shouldBe EntryExists(s"${fid}.2")
    }

    "should get 3 values from feed" in {
      val stream = kvs.index.all(fid)
      stream.map(_.toList) shouldBe Right(List(Right(e3.copy(prev="2")), Right(e2.copy(prev="1")), Right(e1)))
    }

    "should not remove unexisting entry from feed" in {
      kvs.index.remove(fid,"5").left.value shouldBe KeyNotFound
    }

    "should remove entry(2) from feed without prev/next" in {
      kvs.index.remove(e2.fid,"2").getOrElse(???)
    }

    "should get 2 values from feed" in {
      val stream = kvs.index.all(fid)
      stream.map(_.toList) shouldBe Right(List(Right(e3.copy(prev="1")), Right(e1)))
    }

    "should remove entry(1) from feed" in {
      kvs.index.remove(fid,"1").getOrElse(???)
    }

    "should get 1 values from feed" in {
      val stream = kvs.index.all(fid)
      stream.map(_.toList) shouldBe Right(List(Right(e3)))
    }

    "should remove entry(3) from feed" in {
      kvs.index.remove(fid,"3").getOrElse(???)
    }

    "should be empty" in {
      kvs.index.all(fid).getOrElse(???) shouldBe empty
    }

    "should not create stack overflow" in {
      val limit = 100
      LazyList.from(1,1).takeWhile( _.<=(limit)).foreach{ n =>
        val toadd = entry(n)
        val added = kvs.index.add(toadd).getOrElse(???)
        (added.fid, added.id) shouldBe ((toadd.fid, n.toString))
      }
      LazyList.from(1,1).takeWhile( _.<=(limit)).foreach{ n =>
        val toremove = entry(n)
        kvs.index.remove(toremove.fid, toremove.id).getOrElse(???)
      }
    }

    "feed should be empty at the end test" in {
      kvs.el.delete[String](s"IdCounter.$fid")
      kvs.index.all(fid).getOrElse(???) shouldBe empty
      kvs.index.delete(fid)
      kvs.index.all(fid) shouldBe (Right(LazyList.empty))
    }
  }
}
