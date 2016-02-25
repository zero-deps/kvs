package mws.rng.store

import java.nio.ByteBuffer

import akka.actor.{Actor, ActorLogging}
import akka.serialization.SerializationExtension
import mws.rng._
import org.iq80.leveldb._
import akka.util.ByteString

case class GetBucketResp(b:Bucket,l: Option[List[Data]])
case class SavingEntity(k: Key, v:Value, nextKey: Option[Key])

class ReadonlyStore(leveldb: DB ) extends Actor with ActorLogging {
  val serialization = SerializationExtension(context.system)
  val hashing = HashingExtension(context.system)

  def bytes(any: Any): Array[Byte] = any match {
    case b: Bucket => ByteBuffer.allocate(4).putInt(b).array()
    case anyRef: AnyRef => serialization.serialize(anyRef).get
  }

  def fromBytesList[T](arr: Array[Byte], clazz : Class[T]): Option[T] = Option(arr) match {
    case Some(a) => Some(serialization.deserialize(a, clazz).get)
    case None => None
  }

  override def receive: Receive = {
    case StoreGet(key) => sender ! GetResp(doGet(key))
    case BucketGet(b) => sender ! GetBucketResp(b,fromBytesList(leveldb.get(bytes(b)),classOf[List[Data]]))
    case Traverse(fid, start, end) =>
      fromBytesList(leveldb.get(bytes(fid)), classOf[List[Value]]) match {
        case Some(feed) => sender() ! feed.slice(start.getOrElse(0), end.getOrElse(feed.size))
        case None => sender() ! Nil
      }
    case GetSavingEntity(k) => 
      val e = fromBytesList(leveldb.get(bytes(k)), classOf[(Value, Option[Key])]) match {
        case None => SavingEntity(k, ByteString("dymmy"), None)
        case Some((v,nextKey)) => SavingEntity(k, v, nextKey)
      }
      sender ! e
    case _ =>    
  }

  def doGet(key:Key): Option[List[Data]] = {
    val bucket = hashing findBucket key
    fromBytesList(leveldb.get(bytes(bucket)), classOf[List[Data]]) match {
      case Some(l) =>
        val sameKey: List[Data] = l.filter(d => d.key.equals(key))
        if (sameKey.isEmpty) None else {
          log.debug(s"[store][get] key = $key, v = $sameKey")
          Some(sameKey)
        }
      case None => None
    }
  }
}
