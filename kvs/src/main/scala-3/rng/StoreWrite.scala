package zd.rng
package store

import akka.actor.{Actor, ActorLogging, Props}
import akka.cluster.{Cluster, VectorClock}
import java.util.Arrays
import leveldbjnr.*
import proto.*
import zd.rng.data.codec.*
import zd.rng.data.{Data, BucketInfo}
import zd.rng.model.{ReplBucketPut, StorePut, StoreDelete}

class WriteStore(leveldb: LevelDb) extends Actor with ActorLogging {
  import context.system
  val config = system.settings.config.getConfig("ring.leveldb").nn

  val ro = ReadOpts()
  val wo = WriteOpts(config.getBoolean("fsync"))
  val hashing = HashingExtension(system)

  val local: Node = Cluster(system).selfAddress

  def get(k: Key): Option[Array[Byte]] = leveldb.get(k, ro).fold(l => throw l, r => r)

  val `:key:` = stob(":key:")
  val `:keys` = stob(":keys")

  override def postStop(): Unit = {
    try { leveldb.close() } catch { case _: Throwable => () }
    ro.close()
    wo.close()
    super.postStop()
  }

  def receive: Receive = {
    case StorePut(data) => 
      doPut(data)
      sender ! "ok"
    case x: StoreDelete => sender ! doDelete(x.key)
    case ReplBucketPut(b, bucketVc, items) => replBucketPut(b, bucketVc, items.toVector)
    case unhandled => log.warning(s"unhandled message: ${unhandled}")
  }

  def replBucketPut(b: Bucket, bucketVc: VectorClock, items: Vector[Data]): Unit = {
    withBatch{ batch =>
      { // updating bucket info
        val bucketId: Key = itob(b) ++ `:keys`
        val bucketInfo = get(bucketId).map(decode[BucketInfo](_))
        val newKeys = items.map(_.key)
        val v = bucketInfo match {
          case Some(x) =>
            BucketInfo(vc=bucketVc, keys=(newKeys++x.keys).distinct)
          case None =>
            BucketInfo(vc=bucketVc, keys=newKeys.distinct)
        }
        batch.put(bucketId, encode(v))
      }
      // saving keys data
      items.foreach{ data =>
        val keyPath: Key = itob(b) ++ `:key:` ++ data.key
        val keyData: Option[Data] = get(keyPath).map(decode[Data](_))
        val v: Option[Data] = MergeOps.forPut(stored=keyData, received=data)
        v.map(v => batch.put(keyPath, encode(v)))
      }
    }
  }

  def doPut(data: Data): Unit = {
    val _ = withBatch{ batch =>
      { // updating bucket info
        val bucketId: Key = itob(data.bucket) ++ `:keys`
        val bucketInfo = get(bucketId).map(decode[BucketInfo](_))
        val v = bucketInfo match {
          case Some(x) if x.keys contains data.key =>
            val vc = x.vc :+ local.toString
            x.copy(vc=vc)
          case Some(x) =>
            val vc = x.vc :+ local.toString
            x.copy(vc=vc, keys=(data.key +: x.keys))
          case None =>
            val vc = emptyVC :+ local.toString
            BucketInfo(vc=vc, keys=Vector(data.key))
        }
        batch.put(bucketId, encode(v))
      }
      // saving key data
      val keyPath: Key = itob(data.bucket) ++ `:key:` ++ data.key
      val keyData: Option[Data] = get(keyPath).map(decode[Data](_))
      val v: Option[Data] = MergeOps.forPut(stored=keyData, received=data)
      v.map(v => batch.put(keyPath, encode(v)))
    }
  }

  def doDelete(key: Key): String = {
    val b = hashing.findBucket(key)
    val b_info = get(itob(b) ++ `:keys`).map(decode[BucketInfo](_))
    b_info match {
      case Some(b_info) =>
        val vc = b_info.vc :+ local.toString
        val keys = b_info.keys.filterNot(xs => Arrays.equals(xs, key))
        withBatch(batch => {
          batch.delete((itob(b) ++ `:key:` ++ key))
          batch.put((itob(b) ++ `:keys`), encode(BucketInfo(vc, keys)))
        })
        "ok"
      case None => 
        "ok"
    }
  }

  def withBatch[R](body: WriteBatch => R): R = {
    val batch = new WriteBatch
    try {
      val r = body(batch)
      leveldb.write(batch, wo)
      r
    } finally {
      batch.close()
    }
  }
}

object WriteStore {
  def props(leveldb: LevelDb): Props = Props(new WriteStore(leveldb))
}

