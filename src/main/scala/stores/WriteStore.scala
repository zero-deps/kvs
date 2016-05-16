package mws.rng.store

import java.nio.ByteBuffer

import akka.actor.{Actor, ActorLogging}
import akka.cluster.VectorClock
import akka.serialization.SerializationExtension
import mws.rng._
import org.iq80.leveldb._

import scala.annotation.tailrec

case class StoreGet(key:Key)
case class StorePut(data:Data)
case class StoreDelete(key:Key)
case class BucketPut(data: List[Data])
case class BucketDelete(b:Bucket)
case class BucketGet(b:Bucket)
case class GetResp(d: Option[List[Data]])
case class PutSavingEntity(k:Key,v:(Value, Option[Key]))
case class GetSavingEntity(k: Key)

case class FeedAppend(fid:String, v:Value,  version: VectorClock)
sealed trait PutStatus
case object Saved extends PutStatus
case class Conflict(broken: List[Data]) extends PutStatus

class WriteStore(leveldb: DB ) extends Actor with ActorLogging {
  
  val config = context.system.settings.config.getConfig("ring.leveldb")
  val serialization = SerializationExtension(context.system)
  val leveldbWriteOptions = new WriteOptions().sync(config.getBoolean("fsync")).snapshot(false)
  val hashing = HashingExtension(context.system)

  def bytes(any: Any): Array[Byte] = any match {
    case b: Bucket => ByteBuffer.allocate(4).putInt(b).array()
    case anyRef: AnyRef => serialization.serialize(anyRef).get
  }

  def fromBytesList[T](arr: Array[Byte], clazz : Class[T]): Option[T] = Option(arr) match {
    case Some(a) => Some(serialization.deserialize(a, clazz).get)
    case None => None
  }

  override def postStop() = {
    leveldb.close()
    super.postStop()
  }

  def receive: Receive = {
    case StorePut(data) => sender ! doPut(data)
    case PutSavingEntity(k:Key,v:(Value, Option[Key])) => {
      withBatch(batch => {
      batch.put(bytes(k), bytes(v))
      })
    }
    case StoreDelete(data) => sender ! doDelete(data)
    case BucketDelete(b) => leveldb.delete(bytes(b), leveldbWriteOptions)
    case BucketPut(data) => sender ! doBucketPut(data)
    case FeedAppend(fid,v,version) =>
      val fidBytes= bytes(fid)
      val feed = fromBytesList(leveldb.get(fidBytes), classOf[List[Value]]).getOrElse(Nil)
      withBatch(batch => {
        batch.put(bytes(fidBytes), bytes(v :: feed))
        batch.put(bytes(s"$fid:version"), bytes(version))
      })
      sender() ! feed.size
    case unhandled => log.warning(s"[store]unhandled message: $unhandled")
  }

  def doBucketPut(data: List[Data]): String = {
    data.headOption.map(elem => withBatch(batch => {
      batch.put(bytes(elem.bucket), bytes(data))
    }))
    "ok"
  }

  def doPut(data: Data): PutStatus = {
    val keyData = fromBytesList(leveldb.get(bytes(s"${data.bucket}${data.key}")), classOf[List[Data]])

    val updated: (PutStatus, List[Data]) = keyData match {
      case None => (Saved, List(data))
      case Some(list) if list.size == 1 & descendant(list.head.vc, data.vc) => (Saved, List(data))
      case Some(list) if list forall (d => descendant(d.vc, data.vc)) =>
        val newVC = (list map (_.vc)).foldLeft(data.vc)((sum, i) => sum.merge(i))
        (Saved, List(data.copy(vc = newVC)))
      case Some(brokenData) => (Conflict(brokenData), data :: brokenData)
    }
    val keysInBucket = fromBytesList(leveldb.get(bytes(data.bucket)), classOf[List[Key]]).getOrElse(Nil)

    withBatch(batch => {
      if(!keysInBucket.contains(data.key)) batch.put(bytes(data.bucket), bytes(data.key :: keysInBucket))
      batch.put(bytes(data.bucket), bytes(updated._2))
    })
    updated._1
  }

  def doDelete(key: Key): String = {
    val b = hashing.findBucket(key)
    val keys = fromBytesList(leveldb.get(bytes(b)), classOf[List[Key]])
    withBatch(batch => {
      batch.delete(bytes(s"$b:$key"))
      batch.put(bytes(b), bytes(keys.filterNot(_ == key)))
    })
    "ok"
  }

  def withBatch[R](body: WriteBatch ⇒ R): R = {
    val batch = leveldb.createWriteBatch()
    try {
      val r = body(batch)
      leveldb.write(batch, leveldbWriteOptions)
      r
    } finally {
      batch.close()
    }
  }

 def descendant(old: VectorClock, candidat: VectorClock) = !(old <> candidat) && (old < candidat || old == candidat)
}

