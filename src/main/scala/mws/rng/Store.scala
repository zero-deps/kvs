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

  private def doGet(key:Key):List[Data] = {
    val bucket = hashing findBucket Left(key)
    val lookup: Option[List[Data]] = Option(leveldb.get(bytes(bucket))) map fromBytesList

    println(s"[store][get] $key -> $lookup")
    lookup match {
      case Some(l) => l filter(data => data._1 == key)
      case None => Nil
    }
  }

  private def doPut(data:Data):String = {
    println(s"${data._1} putting in store")
    val bucket = hashing findBucket Left(data._1)
    
    val lookup = fromBytesList(leveldb.get(bytes(bucket)))
    insert_and_remove(data, lookup)
    "ok"
  }

  private def doDelete(key: Key): String = doGet(key) match {
    case Nil =>
      println(s"[store][del] k=$key not present")
      "error"
    case l: List[Data] =>
      println(s"[store][del] k=$key")
      val bucket = hashing findBucket Left(key)
      l filterNot (data => data._1 == key) match {
        case Nil => leveldb.delete(bytes(bucket))
        case list: List[Data] => withBatch(b => b.put(bytes(bucket), bytes(list)))
      }
      "ok"
  }
  
  private def insert_and_remove(dataPut:Data, stored:List[Data]):Unit = {
    val bucket = hashing findBucket Left(dataPut._1)
    val updated: List[Data] = dataPut :: stored.filter(d => outdated(dataPut, d))
    println(s"[store][put] $dataPut , stored = $stored, filtered = $updated")
    
    withBatch(batch => {
      batch.put(bytes(bucket), bytes(updated))
    })
  }

  private def outdated(dataPut: Data, persisted: Data): Boolean = {
    def old: Boolean = ((persisted._4 <> dataPut._4) || (persisted._4 > dataPut._4))
    def sameKey: Boolean = dataPut._1 == persisted._1
    
    sameKey && old
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

