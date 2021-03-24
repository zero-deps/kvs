package kvs.seq
package test

import zio._, test._, Assertion._
import proto._, macrosapi._

object FeedSpec extends DefaultRunnableSpec {
  def spec = suite("FeedSpec")(
    testM("explicit keys") {
      val fid = Fid1
      val k1 = Key1(1)
      val k2 = Key1(2)
      for {
        _  <- KvsFeed.put(fid, k1, v1)
        _  <- KvsFeed.put(fid, k2, v2)
        _  <- KvsFeed.put(fid, k1, v3)
        x  <- KvsFeed.all(fid).runCollect
      } yield assert(x)(equalTo(Chunk(k2 -> v2, k1 -> v3)))
    }
  , testM("incremental keys") {
      val fid = Fid2
      for {
        _  <- KvsFeed.add(fid, v1)
        _  <- KvsFeed.add(fid, v2)
        x  <- KvsFeed.all(fid).map{case(k,v)=>v}.runCollect
      } yield assert(x)(equalTo(Chunk(v2, v1)))
    }
  ).provideLayerShared(kvsService(4402, dir="feed"))
}

case class Key1(@N(1) x: Int)
object Key1 {
  implicit val key1Codec = caseCodecAuto[Key1]
}
case object Fid1 {
  type Fid1 = Fid1.type
  implicit val fid1Codec = caseCodecAuto[Fid1]
  implicit val fid1KvsFeed: KvsFeed.Manual[Fid1, Key1, Data] = {
    val id = new Array[Byte](8)
    util.Random.nextBytes(id)
    KvsFeed.manualFeed(Bytes.unsafeWrap(id))
  }
}

case class Key2 private (bytes: Bytes) extends AnyVal
case object Fid2 {
  type Fid2 = Fid2.type
  implicit val fid2Codec = caseCodecAuto[Fid2]
  implicit val fid2KvsFeed: KvsFeed.Increment[Fid2, Key2, Data] = {
    val id = new Array[Byte](8)
    util.Random.nextBytes(id)
    KvsFeed.incrementFeed(Bytes.unsafeWrap(id), _.bytes, Key2.apply)
  }
}
