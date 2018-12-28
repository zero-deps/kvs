package mws.kvs

import mws.rng._
import org.scalatest._
import mws.rng.data._

class UnitTest extends FreeSpecLike with Matchers with EitherValues with BeforeAndAfterAll {
  def vc1(v: Long): Vec = Vec("n1", v) 
  def vc2(v: Long): Vec = Vec("n2", v) 

  "merge buckets" - {
    import mws.rng.ReplicationWorker.mergeBucketData
    import mws.rng.msg_repl.{ReplBucketDataItem}
    "empty" in {
      val xs = Nil
      mergeBucketData(xs) should be (empty)
    }
    "single item" in {
      val xs = Seq(
        Data(stob("k1"), bucket=1, lastModified=1, vc=Seq(vc1(1), vc2(1)), stob("v1")),
      )
      val ys = Set(
        ReplBucketDataItem(xs(0).key, Seq(xs(0))),
      )
      mergeBucketData(xs).toSet should be (ys)
    }
    "no conflict" in {
      val xs = Seq(
        Data(stob("k1"), bucket=1, lastModified=1, vc=Seq(vc1(1), vc2(1)), stob("v1")),
        Data(stob("k2"), bucket=1, lastModified=1, vc=Seq(vc1(1), vc2(1)), stob("v2")),
        Data(stob("k3"), bucket=1, lastModified=1, vc=Seq(vc1(1), vc2(1)), stob("v3")),
      )
      val ys = Set(
        ReplBucketDataItem(xs(0).key, Seq(xs(0))),
        ReplBucketDataItem(xs(1).key, Seq(xs(1))),
        ReplBucketDataItem(xs(2).key, Seq(xs(2))),
      )
      mergeBucketData(xs).toSet should be (ys)
    }
    "same vc" - {
      val vcs = Seq(vc1(1), vc2(1))
      "old then new" in {
        val xs = Seq(
          Data(stob("k1"), bucket=1, lastModified=1, vcs, stob("v11")),
          Data(stob("k1"), bucket=1, lastModified=2, vcs, stob("v12")),
          Data(stob("k2"), bucket=1, lastModified=1, vcs, stob("v2")),
        )
        val ys = Set(
          ReplBucketDataItem(xs(1).key, Seq(xs(1))),
          ReplBucketDataItem(xs(2).key, Seq(xs(2))),
        )
        mergeBucketData(xs).toSet should be (ys)
      }
      "new then old" in {
        val xs = Seq(
          Data(stob("k1"), bucket=1, lastModified=2, vcs, stob("v11")),
          Data(stob("k1"), bucket=1, lastModified=1, vcs, stob("v12")),
          Data(stob("k2"), bucket=1, lastModified=1, vcs, stob("v2")),
        )
        val ys = Set(
          ReplBucketDataItem(xs(0).key, Seq(xs(0))),
          ReplBucketDataItem(xs(2).key, Seq(xs(2))),
        )
        mergeBucketData(xs).toSet should be (ys)
      }
    }
    "new vc" - {
      val vc1s = Seq(vc1(1), vc2(1))
      val vc2s = Seq(vc1(2), vc2(2))
      "old then new" in {
        val xs = Seq(
          Data(stob("k1"), bucket=1, lastModified=2, vc1s, stob("v11")),
          Data(stob("k1"), bucket=1, lastModified=1, vc2s, stob("v12")),
          Data(stob("k2"), bucket=1, lastModified=1, vc1s, stob("v2")),
        )
        val ys = Set(
          ReplBucketDataItem(xs(1).key, Seq(xs(1))),
          ReplBucketDataItem(xs(2).key, Seq(xs(2))),
        )
        mergeBucketData(xs).toSet should be (ys)
      }
      "new then old" in {
        val xs = Seq(
          Data(stob("k1"), bucket=1, lastModified=1, vc2s, stob("v11")),
          Data(stob("k1"), bucket=1, lastModified=2, vc1s, stob("v12")),
          Data(stob("k2"), bucket=1, lastModified=1, vc1s, stob("v2")),
        )
        val ys = Set(
          ReplBucketDataItem(xs(0).key, Seq(xs(0))),
          ReplBucketDataItem(xs(2).key, Seq(xs(2))),
        )
        mergeBucketData(xs).toSet should be (ys)
      }
    }
    "conflict" - {
      val vc1s = Seq(vc1(1), vc2(2))
      val vc2s = Seq(vc1(2), vc2(1))
      "seq" in {
        val xs = Seq(
          Data(stob("k1"), bucket=1, lastModified=2, vc1s, stob("v11")),
          Data(stob("k1"), bucket=1, lastModified=1, vc2s, stob("v12")),
          Data(stob("k2"), bucket=1, lastModified=1, vc1s, stob("v2")),
        )
        val ys = Set(
          ReplBucketDataItem(xs(0).key, Seq(xs(0), xs(1))),
          ReplBucketDataItem(xs(2).key, Seq(xs(2))),
        )
        mergeBucketData(xs).toSet should be (ys)
      }
      "reversed" in {
        val xs = Seq(
          Data(stob("k1"), bucket=1, lastModified=1, vc2s, stob("v11")),
          Data(stob("k1"), bucket=1, lastModified=2, vc1s, stob("v12")),
          Data(stob("k2"), bucket=1, lastModified=1, vc1s, stob("v2")),
        )
        val ys = Set(
          ReplBucketDataItem(xs(0).key, Seq(xs(0), xs(1))),
          ReplBucketDataItem(xs(2).key, Seq(xs(2))),
        )
        mergeBucketData(xs).toSet should be (ys)
      }
    }
  }
}
