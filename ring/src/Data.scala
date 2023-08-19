package kvs.rng
package data

import proto.*

import org.apache.pekko.cluster.given

sealed trait StoreKey

@N(1) final case class DataKey(@N(1) bucket: Int, @N(2) key: Array[Byte]) extends StoreKey

final case class Data
  ( @N(1) lastModified: Long
  , @N(2) vc: VectorClock
  , @N(3) value: Array[Byte]
  )

@N(2) final case class BucketInfoKey(@N(1) bucket: Int) extends StoreKey

final case class BucketInfo
  ( @N(1) vc: VectorClock
  , @N(2) keys: Vector[Array[Byte]]
  )

object keycodec {
  implicit val StoreKeyC: MessageCodec[StoreKey] = {
    implicit val BucketInfoKeyC: MessageCodec[BucketInfoKey] = caseCodecAuto
    implicit val DataKeyC: MessageCodec[DataKey] = caseCodecAuto
    sealedTraitCodecAuto[StoreKey]
  }
}

object codec {
  implicit val bucketInfoCodec: MessageCodec[BucketInfo] = caseCodecAuto
  implicit val dataCodec: MessageCodec[Data] = caseCodecAuto
}
