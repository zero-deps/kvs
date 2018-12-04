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
// import com.github.plokhotnyuk.jsoniter_scala.macros._
// import com.github.plokhotnyuk.jsoniter_scala.core._

// case class GetBucketResp(b:Bucket,l: List[Data])
// case class SavingEntity(k: Key, v:Value, nextKey: Option[Key])

class ReadonlyStore(leveldb: LevelDB) extends Actor with ActorLogging {
  val hashing = HashingExtension(context.system)
  val ro = new LevelDBReadOptions

  // val VectorClockCodec: Codec[VectorClock] = caseCodec1[List[(String, Long)], VectorClock](l => new VectorClock(TreeMap[String, Long]() ++ l), vc => Some(vc.versions.toList))(list(utf8 ~ vlong))
  // val DataCodec: Codec[Data] = caseCodec5(Data.apply, Data.unapply)(abytes, vint, vlong, VectorClockCodec, abytes)

  // val ListDataCodec: Codec[List[Data]] = list(DataCodec)
  // val ListKeyCodec: Codec[List[Key]] = list(abytes)
  // val ValueKeyCodec: Codec[(Value, Option[Key])] = abytes ~ option(abytes)

  // def err(x: scodec.Err): Nothing = throw new Exception(x.messageWithContext)

  def get(k: Key): Option[Array[Byte]] = Option(leveldb.get(k.toByteArray, ro))
  // def decode[A](c: Codec[A])(b: BitVector): A = c.decode(b).fold(err, _.value)

  // val listDataCodec: JsonValueCodec[List[Data]] = JsonCodecMaker.make[List[Data]](CodecMakerConfig())
  // val listKeyCodec: JsonValueCodec[List[Key]] = JsonCodecMaker.make[List[Key]](CodecMakerConfig())
  // val valueKeyCodec: JsonValueCodec[(Value, Option[Key])] = JsonCodecMaker.make[(Value, Option[Key])](CodecMakerConfig())

  val keyWord = stob(":key:")
  val keysWord = stob(":keys")

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
      val e = get(k).map(ValueKey.parseFrom(_)) match {
        case None => SavingEntity(k, stob("dymmy"), ByteString.EMPTY)
        case Some(valueKey) => SavingEntity(k, valueKey.v, valueKey.nextKey)
      }
      sender ! e
    case _ =>    
  }

  override def postStop(): Unit = {
    ro.close()
    super.postStop()
  }
}
