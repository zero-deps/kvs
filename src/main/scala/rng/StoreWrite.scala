package mws.rng
package store

import akka.actor.{Actor, ActorLogging, Props}
import akka.cluster.{Cluster, VectorClock}
import leveldbjnr._
import mws.rng.data.{Data, SeqData, BucketInfo}
import mws.rng.data_dump.{ValueKey}
import mws.rng.msg.{StorePut, StoreDelete, StorePutStatus, StorePutSaved, StorePutConflict}
import mws.rng.msg_repl.{ReplBucketPut, ReplBucketDataItem}
import mws.rng.msg_dump.{DumpPut}
import scalaz.Scalaz._

object WriteStore {
  def props(leveldb: LevelDB): Props = Props(new WriteStore(leveldb))
}

class WriteStore(leveldb: LevelDB) extends Actor with ActorLogging {
  import context.system
  val config = system.settings.config.getConfig("ring.leveldb")

  val ro = new LevelDBReadOptions
  val wo = new LevelDBWriteOptions
  wo.setSync(config.getBoolean("fsync"))
  val hashing = HashingExtension(system)

  val local: Node = Cluster(system).selfAddress

  def get(k: Key): Option[Array[Byte]] = get(k.toByteArray)
  def get(k: Array[Byte]): Option[Array[Byte]] = Option(leveldb.get(k, ro))

  val `:key:` = stob(":key:")
  val `:keys` = stob(":keys")

  override def preStart(): Unit = {
    // old style to new layout bucket -> List[Data] to bucket:key
    val buckets = context.system.settings.config.getInt("ring.buckets")
    (0 to buckets).foreach(b => {
      val binfo = get(itob(b)).map(SeqData.parseFrom(_).data)
      binfo.map(_.groupBy(_.bucket)) match {
        case None =>
        case Some(m) => m.foreach { case (buk, list) =>
          list map doPut
          leveldb.delete(itob(buk).toByteArray,wo)
        }
      }
    })
  }

  override def postStop(): Unit = {
    leveldb.close()
    ro.close()
    wo.close()
    super.postStop()
  }

  def receive: Receive = {
    case StorePut(Some(data)) => 
      sender ! doPut(data)
    case DumpPut(k: Key, v: Value, nextKey: Key) =>
      withBatch(_.put(k.toByteArray, ValueKey(v=v, nextKey=nextKey).toByteArray))
      sender() ! "done"
    case StoreDelete(data) => sender ! doDelete(data)
    case ReplBucketPut(b, bucketVc, items) => doBulkPut(b, bucketVc, items)
    case unhandled => log.warning(s"unhandled message: ${unhandled}")
  }

  def doBulkPut(b: Bucket, bucketVc: VectorClockList, items: Seq[ReplBucketDataItem]): Unit = {
    withBatch{ batch =>
      { // updating bucket info
        val bucket_id: Key = itob(b) ++ `:keys`
        val bucket_info = get(bucket_id).map(BucketInfo.parseFrom(_))
        val newKeys = items.map(_.key)
        bucket_info match {
          case Some(x) =>
            batch.put(
              bucket_id.toByteArray,
              x.copy(vc=bucketVc, keys=(newKeys++x.keys).distinct).toByteArray
            )
          case None =>
            batch.put(
              bucket_id.toByteArray,
              BucketInfo(vc=bucketVc, keys=newKeys.distinct).toByteArray
            )
        }
      }
      items.map{ case ReplBucketDataItem(key: Key, datas: Seq[Data]) =>
        val keyPath: Key = itob(b) ++ `:key:` ++ key
        val keyData: Option[Seq[Data]] = get(keyPath).map(SeqData.parseFrom(_).data)
        datas match {
          case (data +: Seq()) =>
            keyData match {
              case None => 
                val updated = Seq(data)
                batch.put(
                  keyPath.toByteArray,
                  SeqData(updated).toByteArray
                )
              case Some(h +: Seq()) if makevc(h.vc) <= makevc(data.vc) =>
                val updated = Seq(data)
                batch.put(
                  keyPath.toByteArray,
                  SeqData(updated).toByteArray
                )
              case Some(xs) if xs forall (d => makevc(d.vc) <= makevc(data.vc)) =>
                val newVC = xs.foldLeft(makevc(data.vc))((sum, i) => sum.merge(makevc(i.vc)))
                val updated = Seq(data.copy(vc=fromvc(newVC)))
                batch.put(
                  keyPath.toByteArray,
                  SeqData(updated).toByteArray
                )
              case Some(xs) =>
                val updated = data +: xs
                batch.put(
                  keyPath.toByteArray,
                  SeqData(updated).toByteArray
                )
            }
          case _ =>
            val updated = datas ++ keyData.getOrElse(Nil)
            batch.put(
              keyPath.toByteArray,
              SeqData(updated).toByteArray
            )
        }
      }
    }
  }

  def doPut(data: Data): StorePutStatus = {
    val keyData: Option[Seq[Data]] = get(itob(data.bucket) ++ `:key:` ++ data.key).map(SeqData.parseFrom(_).data)
    val (status, mergedKeyData): (StorePutStatus, Seq[Data]) = keyData match {
      case None => 
        StorePutSaved() -> Seq(data)
      case Some(h +: Seq()) if makevc(h.vc) <= makevc(data.vc) =>
        StorePutSaved() -> Seq(data)
      case Some(xs) if xs forall (d => makevc(d.vc) <= makevc(data.vc)) =>
        val newVC = xs.foldLeft(makevc(data.vc))((sum, i) => sum.merge(makevc(i.vc)))
        StorePutSaved() -> Seq(data.copy(vc=fromvc(newVC)))
      case Some(xs) => 
        val broken = data +: xs
        StorePutConflict(broken) -> broken
    }
    
    withBatch{ batch =>
      // updating bucket info
      val bucket_id: Key = itob(data.bucket) ++ `:keys`
      val bucket_info = get(bucket_id).map(BucketInfo.parseFrom(_))
      bucket_info match {
        case Some(x) if x.keys contains data.key =>
          val vc = fromvc(makevc(x.vc) :+ local.toString)
          batch.put(
            bucket_id.toByteArray,
            x.copy(vc = vc).toByteArray
          )
        case Some(x) =>
          val vc = fromvc(makevc(x.vc) :+ local.toString)
          batch.put(
            bucket_id.toByteArray,
            x.copy(vc = vc, keys = data.key +: x.keys).toByteArray
          )
        case None =>
          val vc = fromvc(new VectorClock() :+ local.toString)
          batch.put(
            bucket_id.toByteArray,
            BucketInfo(vc = vc, keys = Seq(data.key)).toByteArray
          )
      }
      // save key's data
      val key: Key = itob(data.bucket) ++ `:key:` ++ data.key
      batch.put(
        key.toByteArray,
        SeqData(mergedKeyData).toByteArray
      )
    }
    
    status
  }

  def doDelete(key: Key): String = {
    val b = hashing.findBucket(key)
    val b_info = get(itob(b) ++ `:keys`).map(BucketInfo.parseFrom(_))
    b_info match {
      case Some(b_info) =>
        val vc = fromvc(makevc(b_info.vc) :+ local.toString)
        val keys = b_info.keys.filterNot(_ === key)
        withBatch(batch => {
          batch.delete((itob(b) ++ `:key:` ++ key).toByteArray)
          batch.put((itob(b) ++ `:keys`).toByteArray, BucketInfo(vc, keys).toByteArray)
        })
        "ok"
      case None => 
        "ok"
    }
  }

  def withBatch[R](body: LevelDBWriteBatch ⇒ R): R = {
    val batch = new LevelDBWriteBatch
    try {
      val r = body(batch)
      leveldb.write(batch,wo)
      r
    } finally {
      batch.close()
    }
  }
}

