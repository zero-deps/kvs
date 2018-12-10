package mws.rng
package store

import akka.actor.{Actor, ActorLogging, Props}
import akka.cluster.{Cluster, VectorClock}
import com.google.protobuf.{ByteStringWrap}
import leveldbjnr._
import mws.rng.data.{Data, SeqData, ValueKey, SeqVec, BucketInfo}
import mws.rng.msg.{StorePut, PutSavingEntity, StoreDelete, BucketPut}
import scalaz.Scalaz._

sealed trait PutStatus  
case object Saved extends PutStatus
final case class Conflict(broken: Seq[Data]) extends PutStatus

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
    case PutSavingEntity(k: Key, v: Value, nextKey: Key) =>
      withBatch(batch => { batch.put(k.toByteArray, ValueKey(v=v, nextKey=nextKey).toByteArray) })
      sender() ! "done"
    case StoreDelete(data) => sender ! doDelete(data)
    case BucketPut(data) => data map doPut
    case unhandled => log.warning(s"[store]unhandled message: $unhandled")
  }

  def doPut(data: Data): PutStatus = {
    val keyData: Option[Seq[Data]] = get(itob(data.bucket).concat(keyWord).concat(data.key)).map(SeqData.parseFrom(_).data)

    val updated: (PutStatus, Seq[Data]) = keyData match {
      case None => 
        Saved -> Seq(data)
      case Some(list) if list.size == 1 & descendant(makevc(list.head.vc), makevc(data.vc)) => 
        Saved -> Seq(data)
      case Some(list) if list forall (d => descendant(makevc(d.vc), makevc(data.vc))) =>
        val newVC = list.foldLeft(makevc(data.vc))((sum, i) => sum.merge(makevc(i.vc)))
        Saved -> Seq(data.copy(vc = fromvc(newVC)))
      case Some(brokenData) => 
        val broken = data +: brokenData
        Conflict(broken) -> broken
    }
    
    withBatch(batch => {
      val bucket_id: Key = itob(data.bucket).concat(keysWord)
      val bucket_info = get(bucket_id).map(BucketInfo.parseFrom(_)) //todo: do one parse
      bucket_info match {
        case Some(x) if x.keys contains data.key =>
          val vc = makevc(SeqVec.parseFrom(x.vc.newCodedInput).vc)
          val vc1 = atob(SeqVec(fromvc(vc :+ local.toString)).toByteArray)
          batch.put(
            bucket_id.toByteArray,
            x.copy(vc = vc1).toByteArray
          )
        case Some(x) =>
          val vc = makevc(SeqVec.parseFrom(x.vc.newCodedInput).vc)
          val vc1 = atob(SeqVec(fromvc(vc :+ local.toString)).toByteArray)
          batch.put(
            bucket_id.toByteArray,
            x.copy(vc = vc1, keys = data.key +: x.keys).toByteArray
          )
        case None =>
          val vc = new VectorClock
          val vc1 = atob(SeqVec(fromvc(vc :+ local.toString)).toByteArray)
          batch.put(
            bucket_id.toByteArray,
            BucketInfo(vc = vc1, keys = Seq(data.key)).toByteArray
          )
      }
      
      val bucket_key: Key = itob(data.bucket).concat(keyWord).concat(data.key)
      batch.put(
        bucket_key.toByteArray,
        SeqData(updated._2).toByteArray
      )
    })
    updated._1
  }

  def doDelete(key: Key): String = {
    val b = hashing.findBucket(key)
    val b_info = get(itob(b).concat(keysWord)).map(BucketInfo.parseFrom(_))
    b_info match {
      case Some(b_info) =>
        val vc = atob(SeqVec(fromvc(makevc(SeqVec.parseFrom(b_info.vc.newCodedInput).vc) :+ local.toString)).toByteArray)
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

