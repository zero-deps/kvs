package mws.kvs

import mws.rng._
import org.scalatest._
import mws.rng.data._

class MergeTest extends FreeSpecLike with Matchers with EitherValues with BeforeAndAfterAll {
  def v1(v: Long): Vec = Vec("n1", v) 
  def v2(v: Long): Vec = Vec("n2", v) 

  "merge buckets" - {
    import mws.rng.MergeOps.forRepl
    "empty" in {
      val xs = Vector.empty
      forRepl(xs) should be (empty)
    }
    "single item" in {
      val xs = Vector(
        Data(stob("k1"), bucket=1, lastModified=1, vc=Vector(v1(1), v2(1)), stob("v1")),
      )
      val ys = Set(
        xs(0),
      )
      forRepl(xs).toSet should be (ys)
    }
    "no conflict" in {
      val xs = Vector(
        Data(stob("k1"), bucket=1, lastModified=1, vc=Vector(v1(1), v2(1)), stob("v1")),
        Data(stob("k2"), bucket=1, lastModified=1, vc=Vector(v1(1), v2(1)), stob("v2")),
        Data(stob("k3"), bucket=1, lastModified=1, vc=Vector(v1(1), v2(1)), stob("v3")),
      )
      val ys = Set(
        xs(0),
        xs(1),
        xs(2),
      )
      forRepl(xs).toSet should be (ys)
    }
    "same vc" - {
      val vcs = Vector(v1(1), v2(1))
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
        forRepl(xs).toSet should be (ys)
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
        forRepl(xs).toSet should be (ys)
      }
    }
    "new vc" - {
      val vc1s = Vector(v1(1), v2(1))
      val vc2s = Vector(v1(2), v2(2))
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
        forRepl(xs).toSet should be (ys)
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
        forRepl(xs).toSet should be (ys)
      }
    }
    "conflict" - {
      val vc1s = Vector(v1(1), v2(2))
      val vc2s = Vector(v1(2), v2(1))
      "seq" in {
        val xs = Vector(
          Data(stob("k1"), bucket=1, lastModified=2, vc1s, stob("v11")),
          Data(stob("k1"), bucket=1, lastModified=1, vc2s, stob("v12")),
          Data(stob("k2"), bucket=1, lastModified=1, vc1s, stob("v2")),
        )
        val ys = Set(
          xs(0),
          xs(2),
        )
        forRepl(xs).toSet should be (ys)
      }
      "reversed" in {
        val xs = Vector(
          Data(stob("k1"), bucket=1, lastModified=1, vc2s, stob("v11")),
          Data(stob("k1"), bucket=1, lastModified=2, vc1s, stob("v12")),
          Data(stob("k2"), bucket=1, lastModified=1, vc1s, stob("v2")),
        )
        val ys = Set(
          xs(1),
          xs(2),
        )
        forRepl(xs).toSet should be (ys)
      }
    }
  }

  "forPut" - {
    import mws.rng.MergeOps.forPut
    "stored is none" in {
      val vc = Vector(v1(1))
      val x = Data(stob("k1"), bucket=1, lastModified=1, vc, stob("v1"))
      forPut(None, x) should be (Some(x))
    }
    "stored vc is older" in {
      val vc1 = Vector(v1(1))
      val vc2 = Vector(v1(2))
      val x = Data(stob("k1"), bucket=1, lastModified=2, vc1, stob("v1"))
      val y = Data(stob("k2"), bucket=1, lastModified=1, vc2, stob("v2"))
      forPut(Some(x), y) should be (Some(y))
    }
    "stored vc is newer" in {
      val vc1 = Vector(v1(2))
      val vc2 = Vector(v1(1))
      val x = Data(stob("k1"), bucket=1, lastModified=1, vc1, stob("v1"))
      val y = Data(stob("k2"), bucket=1, lastModified=2, vc2, stob("v2"))
      forPut(Some(x), y) should be (None)
    }
    "vcs are the same" - {
      val vc = Vector(v1(1))
      "direct order" in {
        val x = Data(stob("k1"), bucket=1, lastModified=1, vc, stob("v1"))
        val y = Data(stob("k2"), bucket=1, lastModified=2, vc, stob("v2"))
        forPut(Some(x), y) should be (Some(y))
      }
      "reverse order" in {
        val x = Data(stob("k1"), bucket=1, lastModified=2, vc, stob("v1"))
        val y = Data(stob("k2"), bucket=1, lastModified=1, vc, stob("v2"))
        forPut(Some(x), y) should be (None)
      }
    }
    "vcs in conflict" - {
      val vc1 = Vector(v1(1), v2(2))
      val vc2 = Vector(v1(2), v2(1))
      val mergedvc = fromvc(makevc(vc1) merge makevc(vc2))
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
}
