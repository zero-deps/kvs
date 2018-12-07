package mws.rng.store

import akka.actor.{Actor, ActorLogging}
import akka.cluster.VectorClock
import akka.serialization.SerializationExtension
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import leveldbjnr._
import mws.rng._
import scala.collection.immutable.TreeMap
import mws.rng.msg.{StoreGet, GetResp, BucketGet, GetBucketResp, BucketKeys, GetSavingEntity, SavingEntity}
import mws.rng.data.{Data, SeqData, SeqKey, ValueKey}
import com.google.protobuf.ByteString

class ReadonlyStore(leveldb: LevelDB) extends Actor with ActorLogging {
  val hashing = HashingExtension(context.system)
  val ro = new LevelDBReadOptions

  def get(k: Key): Option[Array[Byte]] = Option(leveldb.get(k.toByteArray, ro))

  val keyWord = stob(":key:")
  val keysWord = stob(":keys")
  val dummy = stob("readonly_dummy")

  override def receive: Receive = {
    case StoreGet(key) =>
      val k = itob(hashing.findBucket(key)).concat(keyWord).concat(key)
      val result: Seq[Data] = get(k).map(SeqData.parseFrom(_).data).getOrElse(Seq.empty[Data])
      sender ! GetResp(result)
    case BucketGet(b) => 
      val k = itob(b).concat(keysWord)
      val keys: Seq[Key] = get(k).map(SeqKey.parseFrom(_).keys).getOrElse(Seq.empty[Key])
      val data = keys.foldLeft(Seq.empty[Data])((acc, key) =>
        get(itob(b).concat(keyWord).concat(key)).map(SeqData.parseFrom(_).data).getOrElse(Nil) ++ acc
      )
      sender ! GetBucketResp(b, data)
    case BucketKeys(b) => 
      sender ! get(itob(b).concat(keysWord)).map(SeqKey.parseFrom(_).keys).getOrElse(Seq.empty[Key])
    case GetSavingEntity(k) =>
      import java.io.{ByteArrayOutputStream, ObjectOutputStream, ObjectInputStream, ByteArrayInputStream}
      val key: Array[Byte] = {
        val bos = new ByteArrayOutputStream
        val out = new ObjectOutputStream(bos)
        out.writeObject(new String(k.toByteArray, "UTF-8"))
        out.close()
        bos.toByteArray
      }
      val data: Array[Byte] = leveldb.get(key, ro)
      val res: SavingEntity = if (data == null) {
        SavingEntity(k, dummy, ByteString.EMPTY)
      } else {
        val in = new ObjectInputStream(new ByteArrayInputStream(data))
        val obj = in.readObject
        in.close()
        val decoded = obj.asInstanceOf[(akka.util.ByteString, Option[String])] 
        SavingEntity(k, atob(decoded._1.toArray), decoded._2.fold(ByteString.EMPTY)(stob))
      }
      sender ! res
    case _ =>    
  }

  override def postStop(): Unit = {
    ro.close()
    super.postStop()
  }
}
