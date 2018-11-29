package mws.rng.store

import akka.actor.{Actor, ActorLogging}
import akka.util.ByteString
import java.io.{ByteArrayOutputStream, ObjectOutputStream, ObjectInputStream, ByteArrayInputStream}
import java.nio.ByteBuffer
import leveldbjnr._
import mws.rng._

case class GetBucketResp(b:Bucket,l: List[Data])
case class SavingEntity(k: Key, v:Value, nextKey: Option[Key])

class ReadonlyStore(leveldb: LevelDB) extends Actor with ActorLogging {
  val hashing = HashingExtension(context.system)
  val ro = new LevelDBReadOptions

  def bytes(any: Any): Array[Byte] = any match {
    case b: Bucket => ByteBuffer.allocate(4).putInt(b).array()
    case anyRef: AnyRef =>
      val bos = new ByteArrayOutputStream
      val out = new ObjectOutputStream(bos)
      out.writeObject(anyRef)
      out.close()
      bos.toByteArray
  }

  def fromBytesList[T](arr: Array[Byte], clazz : Class[T]): Option[T] = Option(arr) match {
    case Some(a) => 
      val in = new ObjectInputStream(new ByteArrayInputStream(a))
      val obj = in.readObject
      in.close()
      Some(obj.asInstanceOf[T])
    case None => None
  }

  override def receive: Receive = {
    case StoreGet(key) =>
      val result = fromBytesList(leveldb.get(bytes(s"${hashing.findBucket(key)}:key:$key"),ro), classOf[List[Data]])
      sender ! GetResp(result)
    case BucketGet(b) => 
      val keys = fromBytesList(leveldb.get(bytes(s"$b:keys"),ro),classOf[List[Key]])
      val data= keys.getOrElse(Nil).foldLeft(List.empty[Data])((acc, key) => 
      fromBytesList(leveldb.get(bytes(s"$b:key:$key"),ro), classOf[List[Data]]).getOrElse(Nil) ::: acc )
      sender ! GetBucketResp(b, data)
    case BucketKeys(b) => 
      sender ! fromBytesList(leveldb.get(bytes(s"$b:keys"),ro),classOf[List[Key]]).getOrElse(Nil)
    case GetSavingEntity(k) => 
      val e = fromBytesList(leveldb.get(bytes(k),ro), classOf[(Value, Option[Key])]) match {
        case None => SavingEntity(k, ByteString("dymmy"), None)
        case Some((v,nextKey)) => SavingEntity(k, v, nextKey)
      }
      sender ! e
    case _ =>    
  }

  override def postStop(): Unit = {
    ro.close()
    super.postStop()
  }
}
