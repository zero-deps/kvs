package mws.rng.store

import java.nio.ByteBuffer

import akka.actor.{Actor, ActorLogging}
import akka.cluster.VectorClock
import akka.serialization.SerializationExtension
import mws.rng._

import leveldbjnr._

case class StoreGet(key:Key)
case class StorePut(data:Data)
case class StoreDelete(key:Key)
case class BucketPut(data: List[Data])
case class BucketGet(b:Bucket)
case class GetResp(d: Option[List[Data]])
case class PutSavingEntity(k:Key,v:(Value, Option[Key]))
case class GetSavingEntity(k: Key)
case class BucketKeys(b: Bucket)

case class FeedAppend(fid:String, v:Value,  version: VectorClock)
sealed trait PutStatus  
case object Saved extends PutStatus
case class Conflict(broken: List[Data]) extends PutStatus

class WriteStore(leveldb: LevelDB) extends Actor with ActorLogging {
  
  val config = context.system.settings.config.getConfig("ring.leveldb")
  val serialization = SerializationExtension(context.system)
  val ro = new LevelDBReadOptions
  val wo = new LevelDBWriteOptions
  wo.setSync(config.getBoolean("fsync"))
  val hashing = HashingExtension(context.system)

  def bytes(any: Any): Array[Byte] = any match {
    case b: Bucket => ByteBuffer.allocate(4).putInt(b).array()
    case anyRef: AnyRef => serialization.serialize(anyRef).get
  }

  def fromBytesList[T](arr: Array[Byte], clazz : Class[T]): Option[T] = Option(arr) match {
    case Some(a) => Some(serialization.deserialize(a, clazz).get)
    case None => None
  }

  override def preStart(): Unit = {
    // old style to new layout bucket -> List[Data] to bucket:key
    val buckets = context.system.settings.config.getInt("ring.buckets")
    (0 to buckets).foreach(b => {
      val binfo = fromBytesList(leveldb.get(bytes(b),ro), classOf[List[Data]])
      binfo.map(_.groupBy(_.bucket)) match {
        case None =>
        case Some(m) => m.foreach { case (buk, list) =>
          list map doPut
          leveldb.delete(bytes(buk),wo)
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
    case StorePut(data) => 
      sender ! doPut(data)
    case PutSavingEntity(k:Key,v:(Value, Option[Key])) =>
      withBatch(batch => { batch.put(bytes(k), bytes(v)) })
      sender() ! "done"
    case StoreDelete(data) => sender ! doDelete(data)
    case BucketPut(data) => data map doPut
    case unhandled => log.warning(s"[store]unhandled message: $unhandled")
  }

  def doPut(data: Data): PutStatus = {
    val keyData = fromBytesList(leveldb.get(bytes(s"${data.bucket}:key:${data.key}"),ro), classOf[List[Data]])

    val updated: (PutStatus, List[Data]) = keyData match {
      case None => (Saved, List(data))
      case Some(list) if list.size == 1 & descendant(list.head.vc, data.vc) => (Saved, List(data))
      case Some(list) if list forall (d => descendant(d.vc, data.vc)) =>
        val newVC = (list map (_.vc)).foldLeft(data.vc)((sum, i) => sum.merge(i))
        (Saved, List(data.copy(vc = newVC)))
      case Some(brokenData) => 
        val broken = data :: brokenData
        (Conflict(broken), broken)
    }
    
    val keysInBucket = fromBytesList(leveldb.get(bytes(s"${data.bucket}:keys"),ro), classOf[List[Key]]).getOrElse(Nil)

    withBatch(batch => {
      if(!keysInBucket.contains(data.key)) batch.put(bytes(s"${data.bucket}:keys"), bytes(data.key :: keysInBucket))
      batch.put(bytes(s"${data.bucket}:key:${data.key}"), bytes(updated._2))
    })
    updated._1
  }

  def doDelete(key: Key): String = {
    val b = hashing.findBucket(key)
    val keys = fromBytesList(leveldb.get(bytes(s"$b:keys"),ro), classOf[List[Key]])
    val newKeys = keys.getOrElse(Nil).filterNot(_ == key)

    withBatch(batch => {
      batch.delete(bytes(s"$b:key:$key"))
      batch.put(bytes(s"$b:keys"), bytes(newKeys))
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

