package mws.rng
package store

import akka.actor.{Actor, ActorLogging, Props}
import akka.cluster.{Cluster, VectorClock}
import leveldbjnr._
import mws.rng.data.{Data, SeqData, BucketInfo}
import mws.rng.data_dump.{ValueKey}
import mws.rng.msg.{StorePut, StoreDelete, StorePutStatus, StorePutSaved, StorePutConflict}
import mws.rng.msg_repl.{ReplBucketPut}
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

  val keyWord = stob(":key:")
  val keysWord = stob(":keys")

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
    case ReplBucketPut(data, vc) => doBulkPut(data, vc)
    case unhandled => log.warning(s"unhandled message: ${unhandled}")
  }

  def doBulkPut(datas: Seq[Data], vc: VectorClockList): Unit = {
    withBatch{ batch =>
      // update bucket with provided vector clock
      val newKeys = datas.map(_.key)
      datas.map(_.bucket).distinct.foreach{ bucket =>
        val bucket_id: Key = itob(bucket).concat(keysWord)
        val bucket_info = get(bucket_id).map(BucketInfo.parseFrom(_))
        bucket_info match {
          case Some(x) =>
            batch.put(
              bucket_id.toByteArray,
              x.copy(vc = vc, keys = (newKeys ++ x.keys).distinct).toByteArray
            )
          case None =>
            batch.put(
              bucket_id.toByteArray,
              BucketInfo(vc = vc, keys = newKeys).toByteArray
            )
        }
      }
      // save all keys' data
      val updatedDatas: Seq[(Data, Seq[Data])] = datas.map{ data =>
        val keyData: Option[Seq[Data]] = get(itob(data.bucket).concat(keyWord).concat(data.key)).map(SeqData.parseFrom(_).data)
        keyData match {
          case None => 
            data -> Seq(data)
          case Some(list) if list.size == 1 & descendant(makevc(list.head.vc), makevc(data.vc)) => 
            data -> Seq(data)
          case Some(list) if list forall (d => descendant(makevc(d.vc), makevc(data.vc))) =>
            val newVC = list.foldLeft(makevc(data.vc))((sum, i) => sum.merge(makevc(i.vc)))
            data -> Seq(data.copy(vc = fromvc(newVC)))
          case Some(brokenData) =>
            val broken = data +: brokenData
            data -> broken
        }
      }
      updatedDatas.foreach{ case (data, updated) =>
        val bucket_key: Key = itob(data.bucket).concat(keyWord).concat(data.key)
        batch.put(
          bucket_key.toByteArray,
          SeqData(updated).toByteArray
        )
      }
    }
  }

  def doPut(data: Data): StorePutStatus = {
    val keyData: Option[Seq[Data]] = get(itob(data.bucket).concat(keyWord).concat(data.key)).map(SeqData.parseFrom(_).data)

    val updated: (StorePutStatus, Seq[Data]) = keyData match {
      case None => 
        StorePutSaved() -> Seq(data)
      case Some(list) if list.size === 1 & descendant(makevc(list.head.vc), makevc(data.vc)) => 
        StorePutSaved() -> Seq(data)
      case Some(list) if list.forall(d => descendant(makevc(d.vc), makevc(data.vc))) =>
        val newVC = list.foldLeft(makevc(data.vc))((sum, i) => sum.merge(makevc(i.vc)))
        StorePutSaved() -> Seq(data.copy(vc=fromvc(newVC)))
      case Some(brokenData) => 
        val broken = data +: brokenData
        StorePutConflict(broken) -> broken
    }
    
    withBatch{ batch =>
      // update bucket info
      val bucket_id: Key = itob(data.bucket).concat(keysWord)
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
      val bucket_key: Key = itob(data.bucket).concat(keyWord).concat(data.key)
      batch.put(
        bucket_key.toByteArray,
        SeqData(updated._2).toByteArray
      )
    }
    
    updated._1
  }

  def doDelete(key: Key): String = {
    val b = hashing.findBucket(key)
    val b_info = get(itob(b).concat(keysWord)).map(BucketInfo.parseFrom(_))
    b_info match {
      case Some(b_info) =>
        val vc = fromvc(makevc(b_info.vc) :+ local.toString)
        val keys = b_info.keys.filterNot(_ === key)
        withBatch(batch => {
          batch.delete(itob(b).concat(keyWord).concat(key).toByteArray)
          batch.put(itob(b).concat(keysWord).toByteArray, BucketInfo(vc, keys).toByteArray)
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

 def descendant(old: VectorClock, candidat: VectorClock) = !(old <> candidat) && (old < candidat || old == candidat)
}

