package mws.kvs    

import akka.actor.{ExtendedActorSystem}
import akka.serialization.{BaseSerializer}
import mws.rng.msg
import mws.rng.msg.Msg.MsgType

class Serializer(val system: ExtendedActorSystem) extends BaseSerializer {

  override def toBinary(o: AnyRef): Array[Byte] = {
    o match {
      case a: mws.rng.msg.StoreGet => msg.Msg(msgType=MsgType.StoreGet(a)).toByteArray
      case a: mws.rng.msg.StorePut => msg.Msg(msgType=MsgType.StorePut(a)).toByteArray
      case a: mws.rng.msg.StoreDelete => msg.Msg(msgType=MsgType.StoreDelete(a)).toByteArray
      case a: mws.rng.msg.BucketPut => msg.Msg(msgType=MsgType.BucketPut(a)).toByteArray
      case a: mws.rng.msg.BucketGet => msg.Msg(msgType=MsgType.BucketGet(a)).toByteArray
      case a: mws.rng.msg.GetResp => msg.Msg(msgType=MsgType.GetResp(a)).toByteArray
      case a: mws.rng.msg.PutSavingEntity => msg.Msg(msgType=MsgType.PutSavingEntity(a)).toByteArray
      case a: mws.rng.msg.GetSavingEntity => msg.Msg(msgType=MsgType.GetSavingEntity(a)).toByteArray
      case a: mws.rng.msg.GetBucketResp => msg.Msg(msgType=MsgType.GetBucketResp(a)).toByteArray
      case a: mws.rng.msg.SavingEntity => msg.Msg(msgType=MsgType.SavingEntity(a)).toByteArray
      case a: mws.rng.ChangeState => msg.Msg(msgType=MsgType.ChangeState(mws.rng.msg.ChangeState(getQuorumState(a)))).toByteArray
      case _ => throw new IllegalArgumentException(s"${getClass.getName} can't serialize [${o}]")
    }
  }

  def getQuorumState(changeState: mws.rng.ChangeState): Int = {
    changeState.s match {
      case mws.rng.Unsatisfied => 1
      case mws.rng.Readonly => 2
      case mws.rng.Effective => 3
      case mws.rng.WriteOnly => 4
      case mws.rng.WeakReadonly => 5
    }
  }

  def quorumState(v: Int): mws.rng.QuorumState = {
    v match {
      case 1 => mws.rng.Unsatisfied
      case 2 => mws.rng.Readonly
      case 3 => mws.rng.Effective
      case 4 => mws.rng.WriteOnly
      case 5 => mws.rng.WeakReadonly
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
      case MsgType.GetBucketResp(m) => m
      case MsgType.SavingEntity(m) => m
      case MsgType.ChangeState(m) => mws.rng.ChangeState(quorumState(m.quorumState))
    }
  }
}
