package mws.rng

import java.io.File
import java.nio.ByteBuffer

import akka.actor.{Actor, ActorLogging}
import akka.serialization.SerializationExtension
import org.iq80.leveldb._


/**
 *
 * Bucket -> List[ Key->List[Data] ]
 *
 * Retrieves Data associated with Key
 * List[Data] = ring_store:get(Key),
 *
 * % Stores Data, which is a variable of data record
 * ring_store:put(Data).
 */


case class StoreListBucket(bucket:Bucket)
case class StoreGet(key:Key)
case class StorePut(data:Data)
case class StoreDelete(key:Key)


class Store extends {val configPath = "ring.leveldb"} with Actor with ActorLogging{
  
  val config = context.system.settings.config.getConfig(configPath)
  val nativeLeveldb = config.getBoolean("native")
  val hashing = HashingExtension(context.system)

  val leveldbOptions = new Options().createIfMissing(true)
  def leveldbReadOptions = new ReadOptions().verifyChecksums(config.getBoolean("checksum"))
  val leveldbWriteOptions = new WriteOptions().sync(config.getBoolean("fsync")).snapshot(false)
  val leveldbDir = new File(config.getString("dir"))
  var leveldb: DB = _

  def leveldbFactory =
    if (nativeLeveldb) org.fusesource.leveldbjni.JniDBFactory.factory
    else org.iq80.leveldb.impl.Iq80DBFactory.factory

  val serialization = SerializationExtension(context.system)

  def bytes(any: Any): Array[Byte] = any match {
    case b: Bucket => ByteBuffer.allocate(4).putInt(b).array()
    case anyRef: AnyRef => serialization.serialize(anyRef).get
  }

  def fromBytesList(arr: Array[Byte]): List[Data] = Option(arr) match {
    case Some(a) => serialization.deserialize(a, classOf[List[Data]]).get
    case None => Nil
  }

  override def preStart() = {
    leveldb = leveldbFactory.open(leveldbDir, if (nativeLeveldb) leveldbOptions else leveldbOptions.compressionType(CompressionType.NONE))
    super.preStart()
  }

  override def postStop() = {
    leveldb.close()
    super.postStop()
  }

  def receive: Receive = {
    case StoreListBucket(bucket) => sender ! doList(bucket)
    case StoreGet(key) => sender ! doGet(key)
    case StorePut(data) => sender ! doPut(data)
    case StoreDelete(data) => sender ! doDelete(data)

  }

  private def doGet(key:Key): Option[Data] = {
    val bucket = hashing findBucket Left(key)
    val lookup: Option[List[Data]] = Option(leveldb.get(bytes(bucket))) map fromBytesList

    log.info(s"[store][get] $key -> $lookup")
    lookup match {
      case Some(l) => l.find(d => d.key.equals(key))
      case None => None
    }
  }

  private def doPut(data:Data):String = {
    log.info(s"[store][put] k = ${data.key} ")
    val bucket = hashing findBucket Left(data.key)
    val lookup = fromBytesList(leveldb.get(bytes(bucket)))
    val updated = data  :: lookup.filter(d => d.key == data.key && d.vc < data.vc)
    
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

  private def doList(bucket:Bucket):List[Data] = {
    // match the storage to get the data
    // - ignore flags and value
    // - key, last_modified, vector_clock, chechsum - should match
   fromBytesList(leveldb.get(bytes(bucket)))
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

