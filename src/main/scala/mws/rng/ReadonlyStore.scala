package mws.rng

import java.nio.ByteBuffer
import akka.actor.{ActorLogging, Actor}
import akka.serialization.SerializationExtension
import org.iq80.leveldb._


class ReadonlyStore(leveldb: DB ) extends Actor with ActorLogging {

  val configPath = "ring.leveldb"
  val config = context.system.settings.config.getConfig(configPath)
  val serialization = SerializationExtension(context.system)
  val leveldbWriteOptions = new WriteOptions().sync(config.getBoolean("fsync")).snapshot(false)
  val hashing = HashingExtension(context.system)
  def leveldbReadOptions = new ReadOptions().verifyChecksums(config.getBoolean("checksum"))

  override def preStart() = {
    super.preStart()    
  }

  override def postStop() = {
    leveldb.close()
    super.postStop()
  }
  
  def bytes(any: Any): Array[Byte] = any match {
    case b: Bucket => ByteBuffer.allocate(4).putInt(b).array()
    case anyRef: AnyRef => serialization.serialize(anyRef).get
  }

  def fromBytesList(arr: Array[Byte]): List[Data] = Option(arr) match {
    case Some(a) => serialization.deserialize(a, classOf[List[Data]]).get
    case None => Nil
  }

  override def receive: Receive = {
    case StoreGet(key) =>
      log.info(s"[get]$self")
      sender ! GetResp(doGet(key))
    case LocalStoreGet(key) => sender ! LocalGetResp(doGet(key))
    case _ =>    
  }

  private def doGet(key:Key): Option[List[Data]] = {
    val bucket = hashing findBucket Left(key)
    val lookup: Option[List[Data]] = Option(leveldb.get(bytes(bucket))) map fromBytesList

    lookup match {
      case Some(l) =>
        val sameKey: List[Data] = l.filter(d => d.key.equals(key))
        if (sameKey.isEmpty) None else {
          log.info(s"[store][get] $key")
          Some(sameKey)
        }
      case None => None
    }
  }
  
}
