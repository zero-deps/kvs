package kvs

import zio.test._, Assertion._
import scala.collection.immutable.{HashSet, TreeMap}
import proto.Bytes
import zero.ext._, option._

import rng._, rng.data._, rng.model.KeyBucketData

object MergeSpec extends DefaultRunnableSpec {
  def v1(v: Long) = "n1" -> v
  def v2(v: Long) = "n2" -> v
  def vc(v: Tuple2[String,Long]*) = new VectorClock(TreeMap.empty[String,Long] ++ v)

  def stob(x: String): Bytes = Bytes.unsafeWrap(x.getBytes)

  import rng.MergeOps.forRepl
  import rng.MergeOps.forPut
  import rng.MergeOps.forGatherGet
    
  import akka.actor.{Address}
  def addr(n: Int): Address = Address("","","",n)

  def spec = suite("MergeSpec")(
    test("forRepl: empty") {
      val xs = Vector.empty
      assert(forRepl(xs))(isEmpty)
    }
  , test("forRepl: single item") {
      val xs = Vector(
        KeyBucketData(key=stob("k1"), bucket=1, data=Data(lastModified=1, vc=vc(v1(1), v2(1)), stob("v1"))),
      )
      val ys = Set(
        xs(0),
      )
      assert(forRepl(xs))(hasSameElements(ys))
    }
  , test("forRepl: no conflict") {
      val xs = Vector(
        KeyBucketData(key=stob("k1"), bucket=1, data=Data(lastModified=1, vc=vc(v1(1), v2(1)), stob("v1"))),
        KeyBucketData(key=stob("k2"), bucket=1, data=Data(lastModified=1, vc=vc(v1(1), v2(1)), stob("v2"))),
        KeyBucketData(key=stob("k3"), bucket=1, data=Data(lastModified=1, vc=vc(v1(1), v2(1)), stob("v3"))),
      )
      val ys = Set(
        xs(0),
        xs(1),
        xs(2),
      )
      assert(forRepl(xs))(hasSameElements(ys))
    }
  , test("forRepl: same vc: old then new") {
      val vcs = vc(v1(1), v2(1))
      val xs = Vector(
        KeyBucketData(key=stob("k1"), bucket=1, data=Data(lastModified=1, vcs, stob("v11"))),
        KeyBucketData(key=stob("k1"), bucket=1, data=Data(lastModified=2, vcs, stob("v12"))),
        KeyBucketData(key=stob("k2"), bucket=1, data=Data(lastModified=1, vcs, stob("v2"))),
      )
      val ys = Set(
        xs(1),
        xs(2),
      )
      assert(forRepl(xs))(hasSameElements(ys))
    }
  , test("forRepl: same vc: new then old" ) {
      val vcs = vc(v1(1), v2(1))
      val xs = Vector(
        KeyBucketData(key=stob("k1"), bucket=1, data=Data(lastModified=2, vcs, stob("v11"))),
        KeyBucketData(key=stob("k1"), bucket=1, data=Data(lastModified=1, vcs, stob("v12"))),
        KeyBucketData(key=stob("k2"), bucket=1, data=Data(lastModified=1, vcs, stob("v2"))),
      )
      val ys = Set(
        xs(0),
        xs(2),
      )
      assert(forRepl(xs))(hasSameElements(ys))
    }
  , test("forRepl: new vc: old then new") {
      val vc1s = vc(v1(1), v2(1))
      val vc2s = vc(v1(2), v2(2))
      val xs = Vector(
        KeyBucketData(key=stob("k1"), bucket=1, data=Data(lastModified=2, vc1s, stob("v11"))),
        KeyBucketData(key=stob("k1"), bucket=1, data=Data(lastModified=1, vc2s, stob("v12"))),
        KeyBucketData(key=stob("k2"), bucket=1, data=Data(lastModified=1, vc1s, stob("v2"))),
      )
      val ys = Set(
        xs(1),
        xs(2),
      )
      assert(forRepl(xs))(hasSameElements(ys))
    }
  , test("forRepl: new vc: new then old") {
      val vc1s = vc(v1(1), v2(1))
      val vc2s = vc(v1(2), v2(2))
      val xs = Vector(
        KeyBucketData(key=stob("k1"), bucket=1, data=Data(lastModified=1, vc2s, stob("v11"))),
        KeyBucketData(key=stob("k1"), bucket=1, data=Data(lastModified=2, vc1s, stob("v12"))),
        KeyBucketData(key=stob("k2"), bucket=1, data=Data(lastModified=1, vc1s, stob("v2"))),
      )
      val ys = Set(
        xs(0),
        xs(2),
      )
      assert(forRepl(xs))(hasSameElements(ys))
    }
  , test("forRepl: conflict: seq") {
      val vc1s = vc(v1(1), v2(2))
      val vc2s = vc(v1(2), v2(1))
      val xs = Vector(
        KeyBucketData(key=stob("k1"), bucket=1, data=Data(lastModified=2, vc1s, stob("v11"))),
        KeyBucketData(key=stob("k1"), bucket=1, data=Data(lastModified=1, vc2s, stob("v12"))),
        KeyBucketData(key=stob("k2"), bucket=1, data=Data(lastModified=1, vc1s, stob("v2"))),
      )
      val ys = Set(
        xs(0).copy(data=xs(0).data.copy(vc=vc(v1(2), v2(2)))),
        xs(2),
      )
      assert(forRepl(xs))(hasSameElements(ys))
    }
  , test("forRepl: conflict: reversed") {
      val vc1s = vc(v1(1), v2(2))
      val vc2s = vc(v1(2), v2(1))
      val xs = Vector(
        KeyBucketData(key=stob("k1"), bucket=1, data=Data(lastModified=1, vc2s, stob("v11"))),
        KeyBucketData(key=stob("k1"), bucket=1, data=Data(lastModified=2, vc1s, stob("v12"))),
        KeyBucketData(key=stob("k2"), bucket=1, data=Data(lastModified=1, vc1s, stob("v2"))),
      )
      val ys = Set(
        xs(1).copy(data=xs(1).data.copy(vc=vc(v1(2), v2(2)))),
        xs(2),
      )
      assert(forRepl(xs))(hasSameElements(ys))
    }

  , test("forPut: stored is none") {
      val vc1 = vc(v1(1))
      val x = Data(lastModified=1, vc1, stob("v1"))
      assert(forPut(None, x))(equalTo(Some(x)))
    }
  , test("forPut: stored vc is older") {
      val vc1 = vc(v1(1))
      val vc2 = vc(v1(2))
      val x = Data(lastModified=2, vc1, stob("v1"))
      val y = Data(lastModified=1, vc2, stob("v2"))
      assert(forPut(Some(x), y))(equalTo(Some(y)))
    }
  , test("forPut: stored vc is newer") {
      val vc1 = vc(v1(2))
      val vc2 = vc(v1(1))
      val x = Data(lastModified=1, vc1, stob("v1"))
      val y = Data(lastModified=2, vc2, stob("v2"))
      assert(forPut(Some(x), y))(isNone)
    }
  , test("forPut: vcs are the same: direct order") {
      val vc1 = vc(v1(1))
      val x = Data(lastModified=1, vc1, stob("v1"))
      val y = Data(lastModified=2, vc1, stob("v2"))
      assert(forPut(Some(x), y))(equalTo(Some(y)))
    }
  , test("forPut: vcs are the same: reverse order") {
      val vc1 = vc(v1(1))
      val x = Data(lastModified=2, vc1, stob("v1"))
      val y = Data(lastModified=1, vc1, stob("v2"))
      assert(forPut(Some(x), y))(isNone)
    }
  , test("forPut: vcs in conflict: direct order") {
      val vc1 = vc(v1(1), v2(2))
      val vc2 = vc(v1(2), v2(1))
      val mergedvc = vc1 merge vc2
      val x = Data(lastModified=1, vc1, stob("v1"))
      val y = Data(lastModified=2, vc2, stob("v2"))
      assert(forPut(Some(x), y))(equalTo(Some(y.copy(vc=mergedvc))))
    }
  , test("forPut: vcs in conflict: reverse order") {
      val vc1 = vc(v1(1), v2(2))
      val vc2 = vc(v1(2), v2(1))
      val mergedvc = vc1 merge vc2
      val x = Data(lastModified=2, vc2, stob("v1"))
      val y = Data(lastModified=1, vc1, stob("v2"))
      assert(forPut(Some(x), y))(equalTo(Some(x.copy(vc=mergedvc))))
    }

  , test("forGatherGet: empty") {
      assert(forGatherGet(Vector.empty))(equalTo((none: Option[Data]) -> HashSet.empty[Node]))
    }
  , test("forGatherGet: newer in tail") {
      val xs = Vector(
        Some(Data(lastModified=1, vc(v1(2)), stob("v1"))) -> addr(1),
        Some(Data(lastModified=1, vc(v1(3)), stob("v2"))) -> addr(2),
        Some(Data(lastModified=1, vc(v1(1)), stob("v3"))) -> addr(3),
      )
      assert(forGatherGet(xs))(equalTo(xs(1)._1 -> HashSet(addr(1), addr(3))))
    }
  , test("forGatherGet: conflict") {
      val xs = Vector(
        Some(Data(lastModified=1, vc(v1(1)), stob("v1"))) -> addr(1),
        Some(Data(lastModified=2, vc(v2(1)), stob("v2"))) -> addr(2),
      )
      assert(forGatherGet(xs))(equalTo(xs(1)._1 -> HashSet(addr(1))))
    }
  , test("forGatherGet: none") {
      val xs = Vector(
        None -> addr(1),
        Some(Data(lastModified=1, vc(v1(3)), stob("v2"))) -> addr(2),
      )
      assert(forGatherGet(xs))(equalTo(xs(1)._1 -> HashSet(addr(1))))
    }
  )
}
