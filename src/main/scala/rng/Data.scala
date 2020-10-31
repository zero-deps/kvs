package kvs
package rng
package data

import zd.proto.api.{N, MessageCodec}
import zd.proto.macrosapi.{caseCodecAuto, sealedTraitCodecAuto}
import zd.proto.Bytes

sealed trait StoreKey

@N(1) final case class DataKey(@N(1) bucket: Int, @N(2) key: Bytes) extends StoreKey

final case class Data
  ( @N(1) lastModified: Long
  , @N(2) vc: VectorClock
  , @N(3) value: Bytes
  )

@N(2) final case class BucketInfoKey(@N(1) bucket: Int) extends StoreKey

final case class BucketInfo
  ( @N(1) vc: VectorClock
  , @N(2) keys: Vector[Bytes]
  )

object keycodec {
  implicit val StoreKeyC: MessageCodec[StoreKey] = {
    implicit val BucketInfoKeyC: MessageCodec[BucketInfoKey] = caseCodecAuto[BucketInfoKey]
    implicit val DataKeyC: MessageCodec[DataKey] = caseCodecAuto[DataKey]
    sealedTraitCodecAuto[StoreKey]
  }
}

object codec {
  implicit val bucketInfoCodec: MessageCodec[BucketInfo] = caseCodecAuto[BucketInfo]
  implicit val dataCodec: MessageCodec[Data] = caseCodecAuto[Data]
}
