package kvs.seq
package test

import zio._, test._, Assertion._
import zd.proto._, macrosapi._
import zero.ext._, option._

object CircularSpec extends DefaultRunnableSpec {
  def spec = suite("CircularSpec")(
    testM("add spec") {
      val bid = Bid1
      for {
              // prerequisites
        x1 <- KvsCircular.all(bid).runCollect
              // add 1
        x2 <- KvsCircular.add(bid, v1)
        x3 <- KvsCircular.get(bid, 1)
        x4 <- KvsCircular.all(bid).runCollect
              // add 2, 3, 4
        _  <- KvsCircular.add(bid, v2)
        _  <- KvsCircular.add(bid, v3)
        _  <- KvsCircular.add(bid, v4)
        x5 <- KvsCircular.all(bid).runCollect
      } yield assert(x1)(equalTo(Chunk.empty)) &&
              assert(x2)(equalTo(())) &&
              assert(x3)(equalTo(v1.some)) &&
              assert(x4)(equalTo(Chunk(v1))) &&
              assert(x5)(equalTo(Chunk(v2,v3,v4)))
    }
  , testM("put spec") {
      val bid = Bid2
      for {
              // prerequisites
        x1 <- KvsCircular.all(bid).runCollect
              // put 1
        x2 <- KvsCircular.put(bid, 1, v1)
        x3 <- KvsCircular.get(bid, 1)
        x4 <- KvsCircular.all(bid).runCollect
              // put 2
        _  <- KvsCircular.put(bid, 2, v2)
              // override 1
        _  <- KvsCircular.put(bid, 1, v3)
        x5 <- KvsCircular.all(bid).runCollect
      } yield assert(x1)(equalTo(Chunk.empty)) &&
              assert(x2)(equalTo(())) &&
              assert(x3)(equalTo(v1.some)) &&
              assert(x4)(equalTo(Chunk(v1))) &&
              assert(x5)(equalTo(Chunk(v2,v3)))
    }
  ).provideLayerShared(kvsService(4401, dir="circular"))
}

case object Bid1 {
  type Bid = Bid1.type
  implicit val fid2Codec = caseCodecAuto[Bid]
  implicit val fid2KvsFeed: KvsCircular.Buffer[Bid,Data] = {
    val id = new Array[Byte](8)
    util.Random.nextBytes(id)
    KvsCircular.buffer(Bytes.unsafeWrap(id), maxSize=3)
  }
}

case object Bid2 {
  type Bid = Bid2.type
  implicit val fid2Codec = caseCodecAuto[Bid]
  implicit val fid2KvsFeed: KvsCircular.Buffer[Bid,Data] = {
    val id = new Array[Byte](8)
    util.Random.nextBytes(id)
    KvsCircular.buffer(Bytes.unsafeWrap(id), maxSize=2)
  }
}
