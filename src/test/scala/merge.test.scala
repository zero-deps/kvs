package zd.kvs

import zd.rng._
import zd.rng.data._
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest._
import scala.collection.immutable.{HashSet, TreeMap}

class MergeTest extends AnyFreeSpec with Matchers with EitherValues with BeforeAndAfterAll {
  def v1(v: Long) = "n1" -> v
  def v2(v: Long) = "n2" -> v
  def vc(v: Tuple2[String,Long]*) = new VectorClock(TreeMap.empty[String,Long] ++ v)

  "forRepl" - {
    import zd.rng.MergeOps.forRepl
    "empty" in {
      val xs = Vector.empty
      forRepl(xs) should be (empty)
    }
    "single item" in {
      val xs = Vector(
        Data(stob("k1"), bucket=1, lastModified=1, vc=vc(v1(1), v2(1)), stob("v1")),
      )
      val ys = Set(
        xs(0),
      )
      forRepl(xs).toSet should be (ys)
    }
    "no conflict" in {
      val xs = Vector(
        Data(stob("k1"), bucket=1, lastModified=1, vc=vc(v1(1), v2(1)), stob("v1")),
        Data(stob("k2"), bucket=1, lastModified=1, vc=vc(v1(1), v2(1)), stob("v2")),
        Data(stob("k3"), bucket=1, lastModified=1, vc=vc(v1(1), v2(1)), stob("v3")),
      )
      val ys = Set(
        xs(0),
        xs(1),
        xs(2),
      )
      forRepl(xs).toSet should be (ys)
    }
    "same vc" - {
      val vcs = vc(v1(1), v2(1))
      "old then new" in {
        val xs = Vector(
          Data(stob("k1"), bucket=1, lastModified=1, vcs, stob("v11")),
          Data(stob("k1"), bucket=1, lastModified=2, vcs, stob("v12")),
          Data(stob("k2"), bucket=1, lastModified=1, vcs, stob("v2")),
        )
        val ys = Set(
          xs(1),
          xs(2),
        )
        assert(forRepl(xs).toSet.size == ys.size)
        forRepl(xs).toSet.zip(ys).foreach{ case (e1, e2) => assert(e1 == e2) }
      }
      "new then old" in {
        val xs = Vector(
          Data(stob("k1"), bucket=1, lastModified=2, vcs, stob("v11")),
          Data(stob("k1"), bucket=1, lastModified=1, vcs, stob("v12")),
          Data(stob("k2"), bucket=1, lastModified=1, vcs, stob("v2")),
        )
        val ys = Set(
          xs(0),
          xs(2),
        )
        assert(forRepl(xs).toSet.size == ys.size)
        forRepl(xs).toSet.zip(ys).foreach{ case (e1, e2) => assert(e1 == e2) }
      }
    }
    "new vc" - {
      val vc1s = vc(v1(1), v2(1))
      val vc2s = vc(v1(2), v2(2))
      "old then new" in {
        val xs = Vector(
          Data(stob("k1"), bucket=1, lastModified=2, vc1s, stob("v11")),
          Data(stob("k1"), bucket=1, lastModified=1, vc2s, stob("v12")),
          Data(stob("k2"), bucket=1, lastModified=1, vc1s, stob("v2")),
        )
        val ys = Set(
          xs(1),
          xs(2),
        )
        assert(forRepl(xs).toSet.size == ys.size)
        forRepl(xs).toSet.zip(ys).foreach{ case (e1, e2) => assert(e1 == e2) }
      }
      "new then old" in {
        val xs = Vector(
          Data(stob("k1"), bucket=1, lastModified=1, vc2s, stob("v11")),
          Data(stob("k1"), bucket=1, lastModified=2, vc1s, stob("v12")),
          Data(stob("k2"), bucket=1, lastModified=1, vc1s, stob("v2")),
        )
        val ys = Set(
          xs(0),
          xs(2),
        )
        assert(forRepl(xs).toSet.size == ys.size)
        forRepl(xs).toSet.zip(ys).foreach{ case (e1, e2) => assert(e1 == e2) }
      }
    }
    "conflict" - {
      val vc1s = vc(v1(1), v2(2))
      val vc2s = vc(v1(2), v2(1))
      "seq" in {
        val xs = Vector(
          Data(stob("k1"), bucket=1, lastModified=2, vc1s, stob("v11")),
          Data(stob("k1"), bucket=1, lastModified=1, vc2s, stob("v12")),
          Data(stob("k2"), bucket=1, lastModified=1, vc1s, stob("v2")),
        )
        val ys = Set(
          xs(0).copy(vc=vc(v1(2), v2(2))),
          xs(2),
        )
        assert(forRepl(xs).toSet.size == ys.size)
        forRepl(xs).toSet.zip(ys).foreach{ case (e1, e2) => assert(e1 == e2) }
      }
      "reversed" in {
        val xs = Vector(
          Data(stob("k1"), bucket=1, lastModified=1, vc2s, stob("v11")),
          Data(stob("k1"), bucket=1, lastModified=2, vc1s, stob("v12")),
          Data(stob("k2"), bucket=1, lastModified=1, vc1s, stob("v2")),
        )
        val ys = Set(
          xs(1).copy(vc=vc(v1(2), v2(2))),
          xs(2),
        )
        assert(forRepl(xs).toSet.size == ys.size)
        forRepl(xs).toSet.zip(ys).foreach{ case (e1, e2) => assert(e1 == e2) }
      }
    }
  }

  "forPut" - {
    import zd.rng.MergeOps.forPut
    "stored is none" in {
      val vc1 = vc(v1(1))
      val x = Data(stob("k1"), bucket=1, lastModified=1, vc1, stob("v1"))
      forPut(None, x) should be (Some(x))
    }
    "stored vc is older" in {
      val vc1 = vc(v1(1))
      val vc2 = vc(v1(2))
      val x = Data(stob("k1"), bucket=1, lastModified=2, vc1, stob("v1"))
      val y = Data(stob("k2"), bucket=1, lastModified=1, vc2, stob("v2"))
      forPut(Some(x), y) should be (Some(y))
    }
    "stored vc is newer" in {
      val vc1 = vc(v1(2))
      val vc2 = vc(v1(1))
      val x = Data(stob("k1"), bucket=1, lastModified=1, vc1, stob("v1"))
      val y = Data(stob("k2"), bucket=1, lastModified=2, vc2, stob("v2"))
      forPut(Some(x), y) should be (None)
    }
    "vcs are the same" - {
      val vc1 = vc(v1(1))
      "direct order" in {
        val x = Data(stob("k1"), bucket=1, lastModified=1, vc1, stob("v1"))
        val y = Data(stob("k2"), bucket=1, lastModified=2, vc1, stob("v2"))
        forPut(Some(x), y) should be (Some(y))
      }
      "reverse order" in {
        val x = Data(stob("k1"), bucket=1, lastModified=2, vc1, stob("v1"))
        val y = Data(stob("k2"), bucket=1, lastModified=1, vc1, stob("v2"))
        forPut(Some(x), y) should be (None)
      }
    }
    "vcs in conflict" - {
      val vc1 = vc(v1(1), v2(2))
      val vc2 = vc(v1(2), v2(1))
      val mergedvc = vc1 merge vc2
      "direct order" in {
        val x = Data(stob("k1"), bucket=1, lastModified=1, vc1, stob("v1"))
        val y = Data(stob("k2"), bucket=1, lastModified=2, vc2, stob("v2"))
        forPut(Some(x), y) should be (Some(y.copy(vc=mergedvc)))
      }
      "reverse order" in {
        val x = Data(stob("k1"), bucket=1, lastModified=2, vc2, stob("v1"))
        val y = Data(stob("k2"), bucket=1, lastModified=1, vc1, stob("v2"))
        forPut(Some(x), y) should be (Some(x.copy(vc=mergedvc)))
      }
    }
  }

  "forGatherGet" - {
    import zd.rng.MergeOps.forGatherGet
    import akka.actor.{Address}
    def addr(n: Int): Address = Address("","","",n)
    "empty" in {
      forGatherGet(Vector.empty) should be (None -> HashSet.empty)
    }
    "newer in tail" in {
      val xs = Vector(
        Some(Data(stob("k1"), bucket=1, lastModified=1, vc(v1(2)), stob("v1"))) -> addr(1),
        Some(Data(stob("k2"), bucket=1, lastModified=1, vc(v1(3)), stob("v2"))) -> addr(2),
        Some(Data(stob("k3"), bucket=1, lastModified=1, vc(v1(1)), stob("v3"))) -> addr(3),
      )
      forGatherGet(xs) should be (xs(1)._1 -> HashSet(addr(1), addr(3)))
    }
    "conflict" in {
      val xs = Vector(
        Some(Data(stob("k1"), bucket=1, lastModified=1, vc(v1(1)), stob("v1"))) -> addr(1),
        Some(Data(stob("k2"), bucket=1, lastModified=2, vc(v2(1)), stob("v2"))) -> addr(2),
      )
      forGatherGet(xs) should be (xs(1)._1 -> HashSet(addr(1)))
    }
    "none" in {
      val xs = Vector(
        None -> addr(1),
        Some(Data(stob("k2"), bucket=1, lastModified=1, vc(v1(3)), stob("v2"))) -> addr(2),
      )
      forGatherGet(xs) should be (xs(1)._1 -> HashSet(addr(1)))
    }
  }
}
