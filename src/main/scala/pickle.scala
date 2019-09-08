package zd.kvs    

import akka.actor.{ExtendedActorSystem}
import akka.serialization.{BaseSerializer}
import zd.kvs.rng.model._
import zd.proto.api.{MessageCodec, encode, decode}
import zd.proto.macrosapi.{sealedTraitCodecAuto, caseCodecAuto, classCodecAuto, caseCodecIdx}

class Serializer(val system: ExtendedActorSystem) extends BaseSerializer {
  implicit val msgCodec: MessageCodec[Msg] = {
    import zd.kvs.rng.data.codec._
    implicit def tuple2IntACodec[A:MessageCodec]: MessageCodec[Tuple2[Int, A]] = caseCodecIdx[Tuple2[Int, A]]

    implicit val replBucketUpToDateCodec: MessageCodec[ReplBucketUpToDate.type] = caseCodecAuto[ReplBucketUpToDate.type]
    implicit val quorumStateCodec: MessageCodec[QuorumState] = {
      import QuorumState._
      implicit val quorumStateUnsatisfiedCodec: MessageCodec[QuorumStateUnsatisfied.type] = caseCodecAuto[QuorumStateUnsatisfied.type]
      implicit val quorumStateReadonlyCodec: MessageCodec[QuorumStateReadonly.type] = caseCodecAuto[QuorumStateReadonly.type]
      implicit val quorumStateEffectiveCodec: MessageCodec[QuorumStateEffective.type] = caseCodecAuto[QuorumStateEffective.type]
      sealedTraitCodecAuto[QuorumState]
    }
    implicit val changeStateCodec: MessageCodec[ChangeState] = caseCodecAuto[ChangeState]
    implicit val dumpBucketDataCodec: MessageCodec[DumpBucketData] = caseCodecAuto[DumpBucketData]
    implicit val dumpGetBucketDataCodec: MessageCodec[DumpGetBucketData] = caseCodecAuto[DumpGetBucketData]
    implicit val replBucketPutCodec: MessageCodec[ReplBucketPut] = caseCodecAuto[ReplBucketPut]
    implicit val replGetBucketIfNewCodec: MessageCodec[ReplGetBucketIfNew] = caseCodecAuto[ReplGetBucketIfNew]
    implicit val replNewerBucketDataCodec: MessageCodec[ReplNewerBucketData] = caseCodecAuto[ReplNewerBucketData]
    implicit val replBucketsVcCodec: MessageCodec[ReplBucketsVc] = caseCodecAuto[ReplBucketsVc]
    implicit val storeDeleteCodec: MessageCodec[StoreDelete] = classCodecAuto[StoreDelete]
    implicit val storeGetCodec: MessageCodec[StoreGet] = classCodecAuto[StoreGet]
    implicit val storeGetAckCodec: MessageCodec[StoreGetAck] = caseCodecAuto[StoreGetAck]
    implicit val storePutCodec: MessageCodec[StorePut] = caseCodecAuto[StorePut]
    implicit val replGetBucketsVcCodec: MessageCodec[ReplGetBucketsVc] = caseCodecAuto[ReplGetBucketsVc]
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
