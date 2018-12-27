package mws.kvs

import mws.rng._
import org.scalatest._
import mws.rng.data._

class UnitTest extends FreeSpecLike with Matchers with EitherValues with BeforeAndAfterAll {
  def vc1(v: Long): Vec = Vec("n1", v) 
  def vc2(v: Long): Vec = Vec("n2", v) 

  "merge buckets" - {
    import mws.rng.ReplicationWorker.mergeBucketData
    "empty" in {
      val xs = Nil
      mergeBucketData(xs) should be (empty)
    }
    "single item" in {
      val xs = List(
        Data(stob("k1"), bucket=1, lastModified=1, vc=List(vc1(1), vc2(1)), stob("v1"))
      )
      mergeBucketData(xs) should be (xs)
    }
    "no conflict" in {
      val xs = List(
        Data(stob("k1"), bucket=1, lastModified=1, vc=List(vc1(1), vc2(1)), stob("v1")),
        Data(stob("k2"), bucket=1, lastModified=1, vc=List(vc1(1), vc2(1)), stob("v2")),
        Data(stob("k3"), bucket=1, lastModified=1, vc=List(vc1(1), vc2(1)), stob("v3")),
      )
      mergeBucketData(xs).toSet should be (xs.toSet)
    }
    "same vc" - {
      val vcs = List(vc1(1), vc2(1))
      "old then new" in {
        val xs = List(
          Data(stob("k1"), bucket=1, lastModified=1, vcs, stob("v11")),
          Data(stob("k1"), bucket=1, lastModified=2, vcs, stob("v12")),
          Data(stob("k2"), bucket=1, lastModified=1, vcs, stob("v2")),
        )
        mergeBucketData(xs).toSet should be (Set(xs(1), xs(2)))
      }
      "new then old" in {
        val xs = List(
          Data(stob("k1"), bucket=1, lastModified=2, vcs, stob("v11")),
          Data(stob("k1"), bucket=1, lastModified=1, vcs, stob("v12")),
          Data(stob("k2"), bucket=1, lastModified=1, vcs, stob("v2")),
        )
        mergeBucketData(xs).toSet should be (Set(xs(0), xs(2)))
      }
    }
    "new vc" - {
      val vc1s = List(vc1(1), vc2(1))
      val vc2s = List(vc1(2), vc2(2))
      "old then new" in {
        val xs = List(
          Data(stob("k1"), bucket=1, lastModified=2, vc1s, stob("v11")),
          Data(stob("k1"), bucket=1, lastModified=1, vc2s, stob("v12")),
          Data(stob("k2"), bucket=1, lastModified=1, vc1s, stob("v2")),
        )
        mergeBucketData(xs).toSet should be (Set(xs(1), xs(2)))
      }
      "new then old" in {
        val xs = List(
          Data(stob("k1"), bucket=1, lastModified=1, vc2s, stob("v11")),
          Data(stob("k1"), bucket=1, lastModified=2, vc1s, stob("v12")),
          Data(stob("k2"), bucket=1, lastModified=1, vc1s, stob("v2")),
        )
        mergeBucketData(xs).toSet should be (Set(xs(0), xs(2)))
      }
    }
    "conflict" - {
      val vc1s = List(vc1(1), vc2(2))
      val vc2s = List(vc1(2), vc2(1))
      "seq" in {
        val xs = List(
          Data(stob("k1"), bucket=1, lastModified=2, vc1s, stob("v11")),
          Data(stob("k1"), bucket=1, lastModified=1, vc2s, stob("v12")),
          Data(stob("k2"), bucket=1, lastModified=1, vc1s, stob("v2")),
        )
        mergeBucketData(xs).toSet should be (Set(xs(0), xs(2)))
      }
      "reversed" in {
        val xs = List(
          Data(stob("k1"), bucket=1, lastModified=1, vc2s, stob("v11")),
          Data(stob("k1"), bucket=1, lastModified=2, vc1s, stob("v12")),
          Data(stob("k2"), bucket=1, lastModified=1, vc1s, stob("v2")),
        )
        mergeBucketData(xs).toSet should be (Set(xs(0), xs(2))) // ! 0 instead of 1
      }
    }
  }
}
