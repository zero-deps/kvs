package kvs
package rng
package store

import akka.actor.{Actor, ActorLogging, Props}
import akka.cluster.{VectorClock}
import org.rocksdb._
import zd.proto.api.{encode, decode}
import zero.ext._, option._

import data.{Data, BucketInfo, StoreKey, DataKey, BucketInfoKey}, data.codec._, data.keycodec._, model.{DumpGetBucketData, DumpBucketData}, model.{ReplBucketsVc, ReplGetBucketIfNew, ReplBucketUpToDate, ReplNewerBucketData, KeyBucketData}, model.{StoreGet, StoreGetAck}, ReplicationSupervisor.ReplGetBucketsVc

object ReadonlyStore {
  def props(db: RocksDB, hashing: Hashing): Props = Props(new ReadonlyStore(db, hashing))
}

class ReadonlyStore(db: RocksDB, hashing: Hashing) extends Actor with ActorLogging {
  def get(k: Key): Option[Array[Byte]] = fromNullable(db.get(k))

  override def receive: Receive = {
    case x: StoreGet =>
      val k = DataKey(bucket=hashing.findBucket(x.key), key=x.key)
      val result: Option[Data] = get(encode[StoreKey](k)).map(decode[Data](_))
      sender ! StoreGetAck(bucket=k.bucket, key=x.key, data=result)

    case DumpGetBucketData(b) => 
      val k = encode[StoreKey](BucketInfoKey(bucket=b))
      val b_info = get(k).map(decode[BucketInfo](_))
      val keys = b_info.map(_.keys).getOrElse(Vector.empty)
      val items = keys.flatMap(key =>
        get(encode[StoreKey](DataKey(bucket=b, key=key))).map(decode[Data](_)).map(data => KeyBucketData(key=key, bucket=b, data=data))
      )
      sender ! DumpBucketData(b, items)
    case ReplGetBucketIfNew(b, vc) =>
      val vc_other: VectorClock = vc
      val k = encode[StoreKey](BucketInfoKey(bucket=b))
      val b_info = get(k).map(decode[BucketInfo](_))
      b_info match {
        case Some(b_info) =>
          val vc_local: VectorClock = b_info.vc
          vc_other == vc_local || vc_other > vc_local match {
            case true => sender ! ReplBucketUpToDate
            case false =>
              val keys = b_info.keys
              val items = keys.flatMap{ key =>
                val data = get(encode[StoreKey](DataKey(bucket=b, key=key))).map(decode[Data](_))
                data.map(data => KeyBucketData(key=key, bucket=b, data=data))
              }
              sender ! ReplNewerBucketData(b_info.vc, items)
          }
        case None =>
          sender ! ReplBucketUpToDate
      }
    case ReplGetBucketsVc(bs) =>
      val bvcs: Bucket Map VectorClock = bs.view.flatMap{ b =>
        val k = encode[StoreKey](BucketInfoKey(bucket=b))
        get(k).map(x => b -> decode[BucketInfo](x).vc)
      }.to(Map)
      sender ! ReplBucketsVc(bvcs)

    case _ =>    
  }
}
