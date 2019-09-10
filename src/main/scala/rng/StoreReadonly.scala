package zd.kvs
package rng
package store

import akka.actor.{Actor, ActorLogging, Props}
import akka.cluster.{VectorClock}
import leveldbjnr._
import zd.kvs.rng.data.{Data, BucketInfo}
import zd.kvs.rng.data.codec._
import zd.kvs.rng.model.{StoreGet, StoreGetAck}
import zd.kvs.rng.model.{DumpGetBucketData, DumpBucketData}
import zd.kvs.rng.model.{ReplGetBucketsVc, ReplBucketsVc, ReplGetBucketIfNew, ReplBucketUpToDate, ReplNewerBucketData}
import zd.proto.api.decode
import zd.kvs.rng.store.codec._
import zd.proto.api.{encode, decode}

object ReadonlyStore {
  def props(leveldb: LevelDb): Props = Props(new ReadonlyStore(leveldb))
}

class ReadonlyStore(leveldb: LevelDb) extends Actor with ActorLogging {
  val hashing = HashingExtension(context.system)
  val ro = ReadOpts()

  def get(k: Key): Option[Array[Byte]] = leveldb.get(k, ro).fold(l => throw l, r => r)

  override def receive: Receive = {
    case x: StoreGet =>
      val k = encode[StoreKey](DataKey(bucket=hashing.findBucket(x.key.unsafeArray), key=x.key))
      val result: Option[Data] = get(k).map(decode[Data](_))
      sender ! StoreGetAck(result)

    case DumpGetBucketData(b) => 
      val k = encode[StoreKey](BucketInfoKey(bucket=b))
      val b_info = get(k).map(decode[BucketInfo](_))
      val keys = b_info.map(_.keys).getOrElse(Vector.empty)
      val items: Vector[Data] = keys.flatMap(key =>
        get(encode[StoreKey](DataKey(bucket=b, key=key))).map(decode[Data](_))
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
              val items: Vector[Data] = keys.view.flatMap(key =>
                get(encode[StoreKey](DataKey(bucket=b, key=key))).map(decode[Data](_))
              ).to(Vector)
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

  override def postStop(): Unit = {
    ro.close()
    super.postStop()
  }
}
