package mws.rng.store

import java.nio.ByteBuffer

import akka.actor.{Actor, ActorLogging}
import akka.serialization.SerializationExtension
import mws.rng._
import org.iq80.leveldb._
import akka.util.ByteString

case class GetBucketResp(b:Bucket,l: List[Data])
case class SavingEntity(k: Key, v:Value, nextKey: Option[Key])

class ReadonlyStore(leveldb: DB ) extends Actor with ActorLogging {
  val serialization = SerializationExtension(context.system)
  val hashing = HashingExtension(context.system)
  val MAX_VERSIONS = context.system.settings.config.getInt("ring.concurrent-versions")
  
  def bytes(any: Any): Array[Byte] = any match {
    case b: Bucket => ByteBuffer.allocate(4).putInt(b).array()
    case anyRef: AnyRef => serialization.serialize(anyRef).get
  }

  def fromBytesList[T](arr: Array[Byte], clazz : Class[T]): Option[T] = Option(arr) match {
    case Some(a) => Some(serialization.deserialize(a, clazz).get)
    case None => None
  }

  override def receive: Receive = {
    case StoreGet(key) =>
      fromBytesList(leveldb.get(bytes(s"${hashing.findBucket(key)}:key:$key")), classOf[List[Data]]) match {
        case None => GetResp(None)
        case Some(d) if d.size < MAX_VERSIONS => GetResp(Some(d))
        case Some(d) => 
          log.warning(s"[store] To many versions(${d.size}) for key $key , cut to $MAX_VERSIONS")
          sender ! GetResp(Option(d.take(MAX_VERSIONS)))
      }
    case BucketGet(b) => 
      val keys = fromBytesList(leveldb.get(bytes(s"$b:keys")),classOf[List[Key]])
      val data = keys.getOrElse(Nil).foldLeft(List.empty[Data])((acc, key) => 
      fromBytesList(leveldb.get(bytes(s"$b:key:$key")), classOf[List[Data]]).map{_.take(MAX_VERSIONS)}.getOrElse(Nil) ::: acc )
      sender ! GetBucketResp(b, data)
    case BucketKeys(b) => 
      sender ! fromBytesList(leveldb.get(bytes(s"$b:keys")),classOf[List[Key]]).getOrElse(Nil)
    case GetSavingEntity(k) => 
      val e = fromBytesList(leveldb.get(bytes(k)), classOf[(Value, Option[Key])]) match {
        case None => SavingEntity(k, ByteString("dymmy"), None)
        case Some((v,nextKey)) => SavingEntity(k, v, nextKey)
      }
      sender ! e
    case _ =>    
  }

}
