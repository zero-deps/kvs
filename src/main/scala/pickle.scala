package mws.kvs    

import akka.actor.{ExtendedActorSystem}
import akka.serialization.{BaseSerializer}
import mws.rng.model._
import zd.proto.api.{MessageCodec, encode, decode}
import zd.proto.macrosapi.{sealedTraitCodecAuto, messageCodecAuto, messageCodecIdx}

class Serializer(val system: ExtendedActorSystem) extends BaseSerializer {


  implicit val msgCodec: MessageCodec[Msg] = {
    import mws.rng.data.codec._
    implicit def tuple2IntACodec[A:MessageCodec]: MessageCodec[Tuple2[Int, A]] = messageCodecIdx[Tuple2[Int, A]]

    implicit val replBucketUpToDateCodec: MessageCodec[ReplBucketUpToDate.type] = messageCodecAuto[ReplBucketUpToDate.type]
    implicit val quorumStateCodec: MessageCodec[QuorumState] = {
      import QuorumState._
      implicit val quorumStateUnsatisfiedCodec: MessageCodec[QuorumStateUnsatisfied.type] = messageCodecAuto[QuorumStateUnsatisfied.type]
      implicit val quorumStateReadonlyCodec: MessageCodec[QuorumStateReadonly.type] = messageCodecAuto[QuorumStateReadonly.type]
      implicit val quorumStateEffectiveCodec: MessageCodec[QuorumStateEffective.type] = messageCodecAuto[QuorumStateEffective.type]
      sealedTraitCodecAuto[QuorumState]
    }
    implicit val changeStateCodec: MessageCodec[ChangeState] = messageCodecAuto[ChangeState]
    implicit val dumpBucketDataCodec: MessageCodec[DumpBucketData] = messageCodecAuto[DumpBucketData]
    implicit val dumpEnCodec: MessageCodec[DumpEn] = messageCodecAuto[DumpEn]
    implicit val dumpGetCodec: MessageCodec[DumpGet] = messageCodecAuto[DumpGet]
    implicit val dumpGetBucketDataCodec: MessageCodec[DumpGetBucketData] = messageCodecAuto[DumpGetBucketData]
    implicit val dumpPutCodec: MessageCodec[DumpPut] = messageCodecAuto[DumpPut]
    implicit val replBucketPutCodec: MessageCodec[ReplBucketPut] = messageCodecAuto[ReplBucketPut]
    implicit val replGetBucketIfNewCodec: MessageCodec[ReplGetBucketIfNew] = messageCodecAuto[ReplGetBucketIfNew]
    implicit val replNewerBucketDataCodec: MessageCodec[ReplNewerBucketData] = messageCodecAuto[ReplNewerBucketData]
    implicit val replVectorClockCodec: MessageCodec[ReplVectorClock] = messageCodecAuto[ReplVectorClock]
    implicit val replBucketsVcCodec: MessageCodec[ReplBucketsVc] = messageCodecAuto[ReplBucketsVc]
    implicit val storeDeleteCodec: MessageCodec[StoreDelete] = messageCodecAuto[StoreDelete]
    implicit val storeGetCodec: MessageCodec[StoreGet] = messageCodecAuto[StoreGet]
    implicit val storeGetAckCodec: MessageCodec[StoreGetAck] = messageCodecAuto[StoreGetAck]
    implicit val storePutCodec: MessageCodec[StorePut] = messageCodecAuto[StorePut]
    implicit val replGetBucketsVcCodec: MessageCodec[ReplGetBucketsVc] = messageCodecAuto[ReplGetBucketsVc]  

    sealedTraitCodecAuto[Msg]
  }

  override def toBinary(o: AnyRef): Array[Byte] = {
    o match {
      case a: Msg => encode(a)
      case _ => throw new IllegalArgumentException(s"${getClass.getName} can't serialize [${o}]")
    }
  }

  override val includeManifest: Boolean = false

  override def fromBinary(data: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
    decode[Msg](data)
  }
}
