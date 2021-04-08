package zd.kvs    

import akka.actor.{ExtendedActorSystem}
import akka.serialization.{BaseSerializer}
import akka.cluster.given
import zd.rng.model.*
import proto.*

class Serializer(val system: ExtendedActorSystem) extends BaseSerializer {
  implicit val msgCodec: MessageCodec[Msg] = {
    implicit def tuple2IntACodec[A:MessageCodec]: MessageCodec[Tuple2[Int, A]] = caseCodecIdx[Tuple2[Int, A]]
    implicit val replBucketUpToDateCodec: MessageCodec[ReplBucketUpToDate.type] = caseCodecAuto[ReplBucketUpToDate.type]
    implicit val quorumStateCodec: MessageCodec[QuorumState] = {
      import QuorumState.*
      implicit val quorumStateUnsatisfiedCodec: MessageCodec[QuorumStateUnsatisfied.type] = caseCodecAuto[QuorumStateUnsatisfied.type]
      implicit val quorumStateReadonlyCodec: MessageCodec[QuorumStateReadonly.type] = caseCodecAuto[QuorumStateReadonly.type]
      implicit val quorumStateEffectiveCodec: MessageCodec[QuorumStateEffective.type] = caseCodecAuto[QuorumStateEffective.type]
      sealedTraitCodecAuto[QuorumState]
    }
    implicit val changeStateCodec: MessageCodec[ChangeState] = caseCodecAuto[ChangeState]
    implicit val dataCodec: MessageCodec[zd.rng.data.Data] = zd.rng.data.codec.dataCodec
    implicit val dumpBucketDataCodec: MessageCodec[DumpBucketData] = caseCodecAuto[DumpBucketData]
    implicit val dumpEnCodec: MessageCodec[DumpEn] = classCodecAuto[DumpEn]
    implicit val dumpGetCodec: MessageCodec[DumpGet] = classCodecAuto[DumpGet]
    implicit val dumpGetBucketDataCodec: MessageCodec[DumpGetBucketData] = caseCodecAuto[DumpGetBucketData]
    implicit val dumpPutCodec: MessageCodec[DumpPut] = classCodecAuto[DumpPut]
    implicit val vcodec: MessageCodec[(String, Long)] = caseCodecNums[Tuple2[String,Long]]("_1"->1,"_2"->2)
    implicit val vccodec: MessageCodec[akka.cluster.VectorClock] = caseCodecNums[akka.cluster.VectorClock]("versions"->1)
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

  override def fromBinary(data: Array[Byte], manifest: Option[Class[?]]): AnyRef = {
    decode[Msg](data)
  }
}
