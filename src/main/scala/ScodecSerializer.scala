package mws.kvs    

import akka.actor.{ExtendedActorSystem}
import akka.cluster.VectorClock
import akka.serialization.{BaseSerializer}
import java.io.{NotSerializableException}
// import mws.rng.Data
// import mws.rng.store.{StoreGet, StorePut, StoreDelete, BucketPut, BucketGet, GetResp, PutSavingEntity, GetSavingEntity, BucketKeys, GetBucketResp, SavingEntity}
import scala.collection.immutable.TreeMap
import mws.rng.msg
import mws.rng.msg.Msg.MsgType
import mws.rng.data
import com.google.protobuf.ByteString

class ScodecSerializer(val system: ExtendedActorSystem) extends BaseSerializer {

  // def toMsgData(d: Data): data.Data = {
  //   model.Data(key=d.key, bucket=d.bucket, lastModified=d.lastModified, vc=d.vc.map(a => model.Vec(key=a._1, value=a._2)), value=d.value)
  // }

  // def fromMsgData(d: model.Data): Data = {
  //   Data(key=d.key, bucket=d.bucket, lastModified=d.lastModified, vc=d.vc.map(a => a.key -> a.value).toList, value=d.value)
  // }

  // def toMsgValueKey(v: (ByteString, Option[ByteString])): model.ValueOptionKey = {
  //   model.ValueOptionKey(value=v._1, v._2.map(a => model.OptionKey(key=a)))
  // }

  // def fromMsgValueKey(v: model.ValueOptionKey): (ByteString, Option[ByteString]) = {
  //   (v.value, v.key.map(_.key))
  // }

  override def toBinary(o: AnyRef): Array[Byte] = {
    o match {
      case a: msg.Msg => a.toByteArray
      case _ => throw new IllegalArgumentException(s"${getClass.getName} can't serialize [${o}]")
    }

    // val m = o match {
    //   case StoreGet(key) => msg.StoreGet(key=key)
    //   case StorePut(data) => msg.StorePut(data=Some(toMsgData(data)))
    //   case StoreDelete(key) => msg.StoreDelete(key=key)
    //   case BucketPut(data) => msg.BucketPut(data=data.map(toMsgData))
    //   case BucketGet(b) => msg.BucketGet(b=b)
    //   case GetResp(d) => msg.GetResp(d=d.map(toMsgData))
    //   case PutSavingEntity(k, v) => msg.PutSavingEntity(k=k, v=Some(toMsgValueKey(v)))
    //   case GetSavingEntity(k) => msg.GetSavingEntity(k)
    //   case BucketKeys(b) => msg.BucketKeys(b)
    //   case GetBucketResp(b, l) => msg.GetBucketResp(b, l.map(toMsgData))
    //   case SavingEntity(k, v, nextKey) => msg.SavingEntity(k=k, v=v, nextKey.map(a => model.OptionKey(key=a)))
    // }
    // m.toByteArray

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
    // readFromArray(data) match {
      
    //   case other => throw new IllegalArgumentException(s"${getClass.getName} can't deserialize [${other.getClass.getName}]")
    // }
  }
}
