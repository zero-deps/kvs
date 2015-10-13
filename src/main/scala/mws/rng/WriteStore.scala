package mws.rng

import java.nio.ByteBuffer
import akka.actor.{ActorRef, Actor, ActorLogging}
import akka.cluster.VectorClock
import akka.serialization.SerializationExtension
import org.iq80.leveldb._
import scala.annotation.tailrec

case class StoreGet(key:Key)
case class LocalStoreGet(key:Key, received: ActorRef)
case class StorePut(data:Data)
case class StoreDelete(key:Key)
case class BucketPut(data: List[Data])
case class BucketDelete(b:Bucket)
case class BucketGet(b:Bucket)
case class GetResp(d: Option[List[Data]])
case class LocalGetResp(d: Option[List[Data]])

sealed trait PutStatus
case object Saved extends PutStatus
case class Conflict(broken: List[Data]) extends PutStatus

//TODO extract all merge functions to conflict_prevent_component

class WriteStore(leveldb: DB ) extends Actor with ActorLogging {
  
  val configPath = "ring.leveldb"
  val config = context.system.settings.config.getConfig(configPath)
  val serialization = SerializationExtension(context.system)
  val leveldbWriteOptions = new WriteOptions().sync(config.getBoolean("fsync")).snapshot(false)
  val hashing = HashingExtension(context.system)
  val bucketsNumber = context.system.settings.config.getInt("ring.buckets")
  
  def leveldbReadOptions = new ReadOptions().verifyChecksums(config.getBoolean("checksum"))

  def bytes(any: Any): Array[Byte] = any match {
    case b: Bucket => ByteBuffer.allocate(4).putInt(b).array()
    case anyRef: AnyRef => serialization.serialize(anyRef).get
  }

  def fromBytesList(arr: Array[Byte]): List[Data] = Option(arr) match {
    case Some(a) => serialization.deserialize(a, classOf[List[Data]]).get
    case None => Nil
  }

  override def preStart() = { 
    super.preStart()
    log.info(s"START REPARING")
    (1 to bucketsNumber) foreach { b =>
     doBucketPut(getBucketData(b))
    }
  }

  override def postStop() = {
    leveldb.close()
    super.postStop()
  }

  def receive: Receive = {
    case BucketGet(b) => sender ! getBucketData(b) // TODO move to readonly
    case StorePut(data) => sender ! doPut(data)
    case StoreDelete(data) => sender ! doDelete(data)
    case BucketDelete(b) => leveldb.delete(bytes(b), leveldbWriteOptions)
    case BucketPut(data) => sender ! doBucketPut(data)
    case unhandled => log.warning(s"[store]unhandled message: $unhandled")
  }

  // this operation uses only after getbucket. So bucket index should be present 
  def doBucketPut(data: List[Data]): String = {
    if (data.nonEmpty) {     
      // TODO iterate through to fix omited bucket number
      withBatch(batch => {
        batch.put(bytes(data.head.bucket), bytes(data))
      })
    }
    "ok"
  }

  private def doPut(data: Data): PutStatus = {
    val dividedLookup = divideByKey(data.key, fromBytesList(leveldb get bytes(data.bucket)))

    val updated: (PutStatus, List[Data]) = dividedLookup._1 match {
      case Nil => (Saved, List(data))
      case list if list.size == 1 & older(list.head.vc, data.vc) =>
        (Saved, List(data.copy(vc = list.head.vc.merge(data.vc))))
      case list if list forall (d => older(d.vc, data.vc)) =>
        val newVC = (list map (_.vc)).foldLeft(data.vc)((sum, i) => sum.merge(i))
        (Saved, List(data.copy(vc = newVC)))
      case brokenData => (Conflict(brokenData), data :: brokenData)
    }

    log.debug(s"[store][put] k-> ${data.key}")
    withBatch(batch => {
      batch.put(bytes(data.bucket), bytes(updated._2 ::: dividedLookup._2))
    })
    updated._1
  }

  def divideByKey(k: Key, data: List[Data]): (List[Data], List[Data]) = {
    @tailrec
    def divide(allData: List[Data], my: List[Data], others: List[Data]): (List[Data], List[Data]) = allData match {
      case Nil => (my, others)
      case h :: t if h.key == k => divide(t, h :: my, others)
      case h :: t if h.key != k => divide(t, my, h :: others)
    }
    divide(data, Nil, Nil)
  }

  private def doDelete(key: Key): String = {
    val b = hashing.findBucket(Left(key))
    val lookup = fromBytesList(leveldb.get(bytes(b)))

    withBatch(batch => {
      batch.put(bytes(b), bytes(lookup.filterNot(d => d.key.equals(key))))
    })
    "ok"
  }

  private def getBucketData(bucket: Bucket): List[Data] = {
    val bucketData = fromBytesList(leveldb.get(bytes(bucket)))
    val merged = mergeData(bucketData, Nil)
    if(bucketData.size > 2) {
      log.debug(s"!!! Fix for b=$bucket, before = ${bucketData.size}, after = ${merged.size}")
    }
    merged
  }

  @tailrec
  private def mergeData(l: List[Data], n: List[Data]): List[Data] = l match {
    case h :: t =>
      n.find(_.key == h.key) match {
        case Some(d) if h.vc == d.vc && h.lastModified > d.lastModified =>
          mergeData(t, h :: n.filterNot(_.key == h.key))
        case Some(d) if h.vc > d.vc =>
          mergeData(t, h :: n.filterNot(_.key == h.key))
        case None => mergeData(t, h :: n)
        case _ => mergeData(t, n)
      }
    case Nil => n
  }

  private def withBatch[R](body: WriteBatch ⇒ R): R = {
    val batch = leveldb.createWriteBatch()
    try {
      val r = body(batch)
      leveldb.write(batch, leveldbWriteOptions)
      r
    } finally {
      batch.close()
    }
  }

  def leveldbSnapshot(): ReadOptions = leveldbReadOptions.snapshot(leveldb.getSnapshot)

  def withIterator[R](body: DBIterator ⇒ R): R = {
    val ro = leveldbSnapshot()
    val iterator = leveldb.iterator(ro)
    try {
      body(iterator)
    } finally {
      iterator.close()
      ro.snapshot().close()
    }
  }

  def older(who: VectorClock, than: VectorClock) = !(who <> than) && (who < than || who == than)
  
}

