package zd.rng
package data

import zd.proto.api.{N, MessageCodec}
import zd.proto.macrosapi.caseCodecAuto

final case class Data
  ( @N(1) key: Array[Byte]
  , @N(2) bucket: Int
  , @N(3) lastModified: Long
  , @N(4) vc: VectorClock
  , @N(5) value: Array[Byte]
  )

final case class BucketInfo
  ( @N(1) vc: VectorClock
  , @N(2) keys: Vector[Array[Byte]]
  )

object codec {
  implicit val bucketInfoCodec: MessageCodec[BucketInfo] = caseCodecAuto[BucketInfo]
  implicit val dataCodec: MessageCodec[Data] = caseCodecAuto[Data]
}
