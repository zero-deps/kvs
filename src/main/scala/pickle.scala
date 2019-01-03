package mws.kvs    

import akka.actor.{ExtendedActorSystem}
import akka.serialization.{BaseSerializer}
import mws.rng.msg
import mws.rng.msg.Msg.MsgType

class Serializer(val system: ExtendedActorSystem) extends BaseSerializer {

  override def toBinary(o: AnyRef): Array[Byte] = {
    o match {
      case a: mws.rng.msg.ChangeState => msg.Msg(msgType=MsgType.ChangeState(a)).toByteArray

      case a: mws.rng.msg.StoreGetAck => msg.Msg(msgType=MsgType.StoreGetAck(a)).toByteArray
      case a: mws.rng.msg.StoreDelete => msg.Msg(msgType=MsgType.StoreDelete(a)).toByteArray
      case a: mws.rng.msg.StoreGet => msg.Msg(msgType=MsgType.StoreGet(a)).toByteArray
      case a: mws.rng.msg.StorePut => msg.Msg(msgType=MsgType.StorePut(a)).toByteArray

      case a: mws.rng.msg_dump.DumpBucketData => msg.Msg(msgType=MsgType.DumpBucketData(a)).toByteArray
      case a: mws.rng.msg_dump.DumpEn => msg.Msg(msgType=MsgType.DumpEn(a)).toByteArray
      case a: mws.rng.msg_dump.DumpGet => msg.Msg(msgType=MsgType.DumpGet(a)).toByteArray
      case a: mws.rng.msg_dump.DumpGetBucketData => msg.Msg(msgType=MsgType.DumpGetBucketData(a)).toByteArray
      case a: mws.rng.msg_dump.DumpPut => msg.Msg(msgType=MsgType.DumpPut(a)).toByteArray

      case a: mws.rng.msg_repl.ReplBucketPut => msg.Msg(msgType=MsgType.ReplBucketPut(a)).toByteArray
      case a: mws.rng.msg_repl.ReplBucketUpToDate => msg.Msg(msgType=MsgType.ReplBucketUpToDate(a)).toByteArray
      case a: mws.rng.msg_repl.ReplGetBucketIfNew => msg.Msg(msgType=MsgType.ReplGetBucketIfNew(a)).toByteArray
      case a: mws.rng.msg_repl.ReplNewerBucketData => msg.Msg(msgType=MsgType.ReplNewerBucketData(a)).toByteArray

      case _ => throw new IllegalArgumentException(s"${getClass.getName} can't serialize [${o}]")
    }
  }

  override val includeManifest: Boolean = false

  override def fromBinary(data: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
    val m = msg.Msg.parseFrom(data)
    def err = throw new IllegalArgumentException(s"${getClass.getName} can't deserialize [${m}]")
    m.msgType match {
      case MsgType.ChangeState(m) => m
      case MsgType.DumpBucketData(m) => m
      case MsgType.DumpEn(m) => m
      case MsgType.DumpGet(m) => m
      case MsgType.DumpGetBucketData(m) => m
      case MsgType.DumpPut(m) => m
      case MsgType.Empty => err
      case MsgType.ReplBucketPut(m) => m
      case MsgType.ReplBucketUpToDate(m) => m
      case MsgType.ReplGetBucketIfNew(m) => m
      case MsgType.ReplNewerBucketData(m) => m
      case MsgType.StoreDelete(m) => m
      case MsgType.StoreGet(m) => m
      case MsgType.StoreGetAck(m) => m
      case MsgType.StorePut(m) => m
    }
  }
}
