package mws.rng

import java.nio.ByteBuffer
import akka.actor.{ActorRef, Actor, ActorLogging}
import akka.serialization.SerializationExtension
import org.iq80.leveldb._
import scala.annotation.tailrec

case class StoreGet(key:Key)
case class LocalStoreGet(key:Key, received: ActorRef)
case class StorePut(data:Data)
case class StoreDelete(key:Key)
case class BucketPut(data: List[Data])
case class BucketDelete(b:Bucket)
case class BucketGet(bucket:Bucket)
case class GetResp(d: Option[List[Data]])
case class LocalGetResp(d: Option[List[Data]])

class WriteStore(leveldb: DB ) extends Actor with ActorLogging {
  
  val configPath = "ring.leveldb"
  val config = context.system.settings.config.getConfig(configPath)
  val serialization = SerializationExtension(context.system)
  val leveldbWriteOptions = new WriteOptions().sync(config.getBoolean("fsync")).snapshot(false)
  val hashing = HashingExtension(context.system)
  val bucket = context.system.settings.config.getInt("ring.buckets")
  
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
    (1 to bucket) foreach { b =>
     doBucketPut(getBucketData(b))
    }
  }

  override def postStop() = {
    leveldb.close()
    super.postStop()
  }

  def receive: Receive = {
    case BucketGet(bucket) => sender ! getBucketData(bucket) // TODO move to readonly
    case StorePut(data) => sender ! doPut(data)
    case StoreDelete(data) => sender ! doDelete(data)
    case BucketDelete(b) => leveldb.delete(bytes(b), leveldbWriteOptions)
    case BucketPut(data) => doBucketPut(data)
    case unhandled => log.warning(s"[store]unhandled message: $unhandled")
  }


  def doBucketPut(data: List[Data]) {
    val keys = data.foldLeft(Set.empty[Key])((acc, d) => acc.+(d.key))
    val bs = data.foldLeft(Set.empty[Bucket])((acc, d) => acc.+(d.bucket))

    if (data.nonEmpty) {
      withBatch(batch => {
        batch.put(bytes(data.head.bucket), bytes(data))
      })
    }
  }


  private def doPut(data:Data):String = {
    val bucket = hashing findBucket Left(data.key)
    val lookup = fromBytesList(leveldb.get(bytes(bucket)))

    val updated = data  :: lookup.filterNot(d => d.key == data.key)
    log.info(s"[store][put] k-> ${data.key}")
    withBatch(batch => {
      batch.put(bytes(bucket), bytes(updated))
    })
    "ok"
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
      log.info(s"!!! Fix for b=${bucket}, before = ${bucketData.size}, after = ${merged.size}")
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

}

