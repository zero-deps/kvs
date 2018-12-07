package mws.kvs    

import akka.actor.{ExtendedActorSystem}
import akka.cluster.VectorClock
import akka.serialization.{BaseSerializer}
import java.io.{NotSerializableException}
import scala.collection.immutable.TreeMap
import mws.rng.msg
import mws.rng.msg.Msg.MsgType
import mws.rng.data
import com.google.protobuf.ByteString

class Serializer(val system: ExtendedActorSystem) extends BaseSerializer {

  override def toBinary(o: AnyRef): Array[Byte] = {
    o match {
      case a: msg.Msg => a.toByteArray
      case _ => throw new IllegalArgumentException(s"${getClass.getName} can't serialize [${o}]")
    }
  }

  override val includeManifest: Boolean = false

  override def fromBinary(data: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
    val m = msg.Msg.parseFrom(data)
    def err = throw new IllegalArgumentException(s"${getClass.getName} can't deserialize [${m}]")
    m.msgType match {
      case MsgType.Empty => err
      case MsgType.StoreGet(m) => m
      case MsgType.StorePut(m) => m
      case MsgType.StoreDelete(m) => m
      case MsgType.BucketPut(m) => m
      case MsgType.BucketGet(m) => m
      case MsgType.GetResp(m) => m
      case MsgType.PutSavingEntity(m) => m
      case MsgType.GetSavingEntity(m) => m
      case MsgType.BucketKeys(m) => m
      case MsgType.GetBucketResp(m) => m
      case MsgType.SavingEntity(m) => m
    }
  }
}
