package mws.rng
package store

import akka.actor.{Actor, ActorLogging}
import akka.cluster.VectorClock
import leveldbjnr._
import mws.rng.data.{Data, SeqData, SeqKey, ValueKey/*, SeqVec*/}
import mws.rng.msg.{StorePut, PutSavingEntity, StoreDelete, BucketPut}
import scalaz.Scalaz._

sealed trait PutStatus  
case object Saved extends PutStatus
final case class Conflict(broken: Seq[Data]) extends PutStatus

class WriteStore(leveldb: LevelDB) extends Actor with ActorLogging {
  val config = context.system.settings.config.getConfig("ring.leveldb")

  val ro = new LevelDBReadOptions
  val wo = new LevelDBWriteOptions
  wo.setSync(config.getBoolean("fsync"))
  val hashing = HashingExtension(context.system)

  def get(k: Key): Option[Array[Byte]] = get(k.toByteArray)
  def get(k: Array[Byte]): Option[Array[Byte]] = Option(leveldb.get(k, ro))

  val keyWord = stob(":key:")
  val keysWord = stob(":keys")
  // val vcWord = stob(":vc")

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
    
    withBatch(batch => {
      val bucket_keys: Key = itob(data.bucket).concat(keysWord)
      val keysInBucket: Option[Seq[Key]] = get(bucket_keys).map(SeqKey.parseFrom(_).keys)
      keysInBucket match {
        case Some(xs) if xs contains data.key => //nop
        case Some(xs) =>
          batch.put(
            bucket_keys.toByteArray, 
            SeqKey(data.key +: xs).toByteArray
          )
        case None =>
          batch.put(
            bucket_keys.toByteArray,
            SeqKey(Seq(data.key)).toByteArray
          )
      }
      
      // val bucket_keys_vc: Key = itob(data.bucket).concat(keysWord).concat(vcWord)
      // val bucket_keys_vc_ba: Array[Byte] = bucket_keys_vc.toByteArray
      // val vcOfBucket = get(bucket_keys_vc_ba).map(SeqVec.parseFrom(_).v).map(makevc).getOrElse(new VectorClock)
      // batch.put(
      //   bucket_keys_vc.toByteArray,
      //   SeqVec(fromvc(vcOfBucket)).toByteArray
      // )
      
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
    val keys = get(itob(b).concat(keysWord)).map(SeqKey.parseFrom(_).keys)
    val newKeys = keys.getOrElse(Nil).filterNot(_ === key)

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

