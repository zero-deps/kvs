package kvs

import zio.test._, Assertion._
import zd.proto.Bytes
import zero.ext._, option._

object ElSpec extends DefaultRunnableSpec {
  implicit val dba = store.Mem()
  
  def spec = suite("ElSpec")(
    testM("get/put/del") {
      for {
              // get absent
        x1 <- el.get(key1)
              // put value
        x2 <- el.put(key1, bs1)
        x3 <- el.get(key1)
              // override value
        x4 <- el.put(key1, bs2)
        x5 <- el.get(key1)
              // delete value
        x6 <- el.del(key1)
        x7 <- el.get(key1)
              // delete again
        x8 <- el.del(key1)
      } yield assert(x1)(equalTo(none))     &&
              assert(x2)(isUnit)            &&
              assert(x3)(equalTo(bs1.some)) &&
              assert(x4)(isUnit)            &&
              assert(x5)(equalTo(bs2.some)) &&
              assert(x6)(isUnit)            &&
              assert(x7)(equalTo(none))     &&
              assert(x8)(isUnit)
    }
  )

  def key(b: Byte): ElKey = ElKey(bs(b))
  def bs (b: Byte): Bytes = Bytes.unsafeWrap(Array(b))
  val key1 = key(1)
  val bs1  = bs(1)
  val bs2  = bs(2)
}
