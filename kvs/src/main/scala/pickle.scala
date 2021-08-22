package kvs    

import akka.actor.{ExtendedActorSystem}
import akka.serialization.{BaseSerializer}
import proto.*

import rng.model.*, rng.data.codec.*
import akka.cluster.given

class Serializer(val system: ExtendedActorSystem) extends BaseSerializer {
  implicit val msgCodec: MessageCodec[Msg] = {
    implicit def tuple2IntACodec[A:MessageCodec]: MessageCodec[Tuple2[Int, A]] = caseCodecIdx[Tuple2[Int, A]]
    implicit val KeyBucketDataC: MessageCodec[KeyBucketData] = caseCodecAuto
    implicit val replBucketUpToDateCodec: MessageCodec[ReplBucketUpToDate.type] = caseCodecAuto
    implicit val quorumStateCodec: MessageCodec[QuorumState] = {
      import QuorumState.*
      implicit val quorumStateUnsatisfiedCodec: MessageCodec[QuorumStateUnsatisfied.type] = caseCodecAuto
      implicit val quorumStateReadonlyCodec: MessageCodec[QuorumStateReadonly.type] = caseCodecAuto
      implicit val quorumStateEffectiveCodec: MessageCodec[QuorumStateEffective.type] = caseCodecAuto
      sealedTraitCodecAuto[QuorumState]
    }
    implicit val changeStateCodec: MessageCodec[ChangeState] = caseCodecAuto
    implicit val dumpBucketDataCodec: MessageCodec[DumpBucketData] = caseCodecAuto
    implicit val dumpGetBucketDataCodec: MessageCodec[DumpGetBucketData] = caseCodecAuto
    implicit val replBucketPutCodec: MessageCodec[ReplBucketPut] = caseCodecAuto
    implicit val replGetBucketIfNewCodec: MessageCodec[ReplGetBucketIfNew] = caseCodecAuto
    implicit val replNewerBucketDataCodec: MessageCodec[ReplNewerBucketData] = caseCodecAuto
    implicit val replBucketsVcCodec: MessageCodec[ReplBucketsVc] = caseCodecAuto
    implicit val storeDeleteCodec: MessageCodec[StoreDelete] = caseCodecAuto
    implicit val storeGetCodec: MessageCodec[StoreGet] = caseCodecAuto
    implicit val storeGetAckCodec: MessageCodec[StoreGetAck] = caseCodecAuto
    implicit val storePutCodec: MessageCodec[StorePut] = caseCodecAuto
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
