package kvs.rng
package store

import org.apache.pekko.actor.{Actor, ActorLogging, Props}
import org.apache.pekko.cluster.{Cluster, VectorClock}
import org.rocksdb.*
import proto.{encode, decode}

import data.codec.*, data.keycodec.*, data.{Data, BucketInfo, StoreKey, DataKey, BucketInfoKey}, model.{ReplBucketPut, StorePut, StoreDelete, KeyBucketData}

object WriteStore {
  def props(db: RocksDB, hashing: Hashing): Props = Props(new WriteStore(db, hashing))
}

class WriteStore(db: RocksDB, hashing: Hashing) extends Actor with ActorLogging {
  import context.system

  val wo = new WriteOptions

  val local: Node = Cluster(system).selfAddress

  def get(k: Key): Option[Array[Byte]] = Option(db.get(k)).map(_.nn)

  override def postStop(): Unit = {
    db.close()
    wo.close()
    super.postStop()
  }

  def receive: Receive = {
    case StorePut(key, bucket, data) => 
      doPut(key, bucket, data)
      sender() ! "ok"
    case x: StoreDelete => sender() ! doDelete(x.key)
    case ReplBucketPut(b, bucketVc, items) => replBucketPut(b, bucketVc, items)
    case unhandled => log.warning(s"unhandled message: ${unhandled}")
  }

  def replBucketPut(b: Bucket, bucketVc: VectorClock, items: Vector[KeyBucketData]): Unit = {
    withBatch{ batch =>
      { // updating bucket info
        val bucketId: Key = encode[StoreKey](BucketInfoKey(bucket=b))
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
        val keyPath: Key = encode[StoreKey](DataKey(bucket=b, key=data.key))
        val keyData: Option[Data] = get(keyPath).map(decode[Data](_))
        val v: Option[Data] = MergeOps.forPut(stored=keyData, received=data.data)
        v.map(v => batch.put(keyPath, encode(v)))
      }
    }
  }

  def doPut(key: Array[Byte], bucket: Int, data: Data): Unit = {
    val _ = withBatch{ batch =>
      { // updating bucket info
        val bucketId: Key = encode[StoreKey](BucketInfoKey(bucket=bucket))
        val bucketInfo = get(bucketId).map(decode[BucketInfo](_))
        val v = bucketInfo match {
          case Some(x) if x.keys contains key =>
            val vc = x.vc :+ local.toString
            x.copy(vc=vc)
          case Some(x) =>
            val vc = x.vc :+ local.toString
            x.copy(vc=vc, keys=(key +: x.keys))
          case None =>
            val vc = emptyVC :+ local.toString
            BucketInfo(vc=vc, keys=Vector(key))
        }
        batch.put(bucketId, encode(v))
      }
      // saving key data
      val keyPath: Key = encode[StoreKey](DataKey(bucket=bucket, key=key))
      val keyData: Option[Data] = get(keyPath).map(decode[Data](_))
      val v: Option[Data] = MergeOps.forPut(stored=keyData, received=data)
      v.map(v => batch.put(keyPath, encode(v)))
    }
  }

  def doDelete(key: Array[Byte]): String = {
    val b = hashing.findBucket(key)
    val b_info = get(encode[StoreKey](BucketInfoKey(bucket=b))).map(decode[BucketInfo](_))
    b_info.foreach{ b_info =>
      val vc = b_info.vc :+ local.toString
      val keys = b_info.keys.filterNot(_ == key)
      withBatch(batch => {
        batch.delete(encode[StoreKey](DataKey(bucket=b, key=key)))
        batch.put(encode[StoreKey](BucketInfoKey(bucket=b)), encode(BucketInfo(vc, keys)))
      })
    }
    "ok"
  }

  def withBatch[R](body: WriteBatch => R): R = {
    val batch = new WriteBatch
    try {
      val r = body(batch)
      db.write(wo, batch)
      r
    } finally {
      batch.close()
    }
  }
}
