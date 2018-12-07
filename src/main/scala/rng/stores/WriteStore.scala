package mws.rng.store

import akka.actor.{Actor, ActorLogging}
import akka.cluster.VectorClock
import akka.serialization.SerializationExtension
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import leveldbjnr._
import mws.rng._
import scala.collection.immutable.TreeMap
import mws.rng.msg.{StoreGet, GetResp, BucketGet, GetBucketResp, BucketKeys, GetSavingEntity, SavingEntity, StorePut, PutSavingEntity, StoreDelete, BucketPut}
import mws.rng.data.{Data, SeqData, SeqKey, ValueKey}

case class FeedAppend(fid: String, v: Value, version: VectorClock)
sealed trait PutStatus  
case object Saved extends PutStatus
case class Conflict(broken: Seq[Data]) extends PutStatus

class WriteStore(leveldb: LevelDB) extends Actor with ActorLogging {
  
  val config = context.system.settings.config.getConfig("ring.leveldb")

  val ro = new LevelDBReadOptions
  val wo = new LevelDBWriteOptions
  wo.setSync(config.getBoolean("fsync"))
  val hashing = HashingExtension(context.system)

  def get(k: Key): Option[Array[Byte]] = Option(leveldb.get(k.toByteArray, ro))

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
      case None => (Saved, Seq(data))
      case Some(list) if list.size == 1 & descendant(makevc(list.head.vc), makevc(data.vc)) => (Saved, Seq(data))
      case Some(list) if list forall (d => descendant(makevc(d.vc), makevc(data.vc))) =>
        val newVC = (list map (_.vc)).foldLeft(makevc(data.vc))((sum, i) => sum.merge(makevc(i)))
        (Saved, Seq(data.copy(vc = fromvc(newVC))))
      case Some(brokenData) => 
        val broken = data +: brokenData
        (Conflict(broken), broken)
    }
    
    val keysInBucket = get(itob(data.bucket).concat(keysWord)).map(SeqKey.parseFrom(_).keys).getOrElse(Nil)

    withBatch(batch => {
      if(!keysInBucket.contains(data.key)) batch.put(itob(data.bucket).concat(keysWord).toByteArray, SeqKey(data.key +: keysInBucket).toByteArray)
      batch.put(itob(data.bucket).concat(keyWord).concat(data.key).toByteArray, SeqData(updated._2).toByteArray)
    })
    updated._1
  }

  def doDelete(key: Key): String = {
    val b = hashing.findBucket(key)
    val keys = get(itob(b).concat(keysWord)).map(SeqKey.parseFrom(_).keys)
    val newKeys = keys.getOrElse(Nil).filterNot(_ == key)

    withBatch(batch => {
      batch.delete(itob(b).concat(keyWord).concat(key).toByteArray)
      batch.put(itob(b).concat(keysWord).toByteArray, SeqKey(newKeys).toByteArray)
    })
    "ok"
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

