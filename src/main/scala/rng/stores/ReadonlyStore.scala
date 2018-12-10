package mws.rng
package store

import akka.actor.{Actor, ActorLogging, Props}
import akka.cluster.{VectorClock}
import com.google.protobuf.ByteString
import leveldbjnr._
import mws.rng.data.{Data, SeqData, BucketInfo, SeqVec}
import mws.rng.msg.{StoreGet, GetResp, BucketGet, GetBucketResp, BucketKeys, GetSavingEntity, SavingEntity, GetBucketVc, BucketVc, GetBucketIfNew, NoChanges}

object ReadonlyStore {
  def props(leveldb: LevelDB): Props = Props(new ReadonlyStore(leveldb))
}

class ReadonlyStore(leveldb: LevelDB) extends Actor with ActorLogging {
  val hashing = HashingExtension(context.system)
  val ro = new LevelDBReadOptions

  def get(k: Key): Option[Array[Byte]] = get(k.toByteArray)
  def get(k: Array[Byte]): Option[Array[Byte]] = Option(leveldb.get(k, ro))

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
      val b_info = get(k).map(BucketInfo.parseFrom(_))
      val keys = b_info.map(_.keys).getOrElse(Nil)
      val data = keys.foldLeft(Nil: Seq[Data])((acc, key) =>
        get(itob(b).concat(keyWord).concat(key)).map(SeqData.parseFrom(_).data).getOrElse(Nil) ++ acc
      )
      sender ! GetBucketResp(b, data)
    case BucketKeys(b) => 
      sender ! get(itob(b).concat(keysWord)).map(BucketInfo.parseFrom(_).keys).getOrElse(Nil: Seq[Key])
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
    case GetBucketVc(b) =>
      val k = itob(b).concat(keysWord)
      val vc = get(k).map(BucketInfo.parseFrom(_).vc)
      sender ! BucketVc(vc)
    case GetBucketIfNew(b, vc) =>
      val vc_other: VectorClock = vc.map(x => makevc(SeqVec.parseFrom(x.newCodedInput).vc)).getOrElse(new VectorClock)
      val k = itob(b).concat(keysWord)
      val b_info = get(k).map(BucketInfo.parseFrom(_))
      b_info match {
        case Some(b_info) =>
          val vc_local: VectorClock = makevc(SeqVec.parseFrom(b_info.vc.newCodedInput).vc)
          vc_other == vc_local || vc_other > vc_local match {
            case true => sender ! NoChanges(b)
            case false =>
              val data = b_info.keys.foldLeft(Nil: Seq[Data])((acc, key) =>
                get(itob(b).concat(keyWord).concat(key)).map(SeqData.parseFrom(_).data).getOrElse(Nil) ++ acc
              )
              sender ! GetBucketResp(b, data)
          }
        case None =>
          sender ! GetBucketResp(b, Nil)
      }
    case _ =>    
  }

  override def postStop(): Unit = {
    ro.close()
    super.postStop()
  }
}
