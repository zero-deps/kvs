package zd.kvs
package rng
package data

import zd.proto.api.{N, MessageCodec}
import zd.proto.macrosapi.{caseCodecAuto}
import zd.proto.Bytes

final case class Data
  ( @N(1) key: Bytes //todo duplicates DataKey
  , @N(2) bucket: Int // todo duplicates DataKey
  , @N(3) lastModified: Long
  , @N(4) vc: VectorClock
  , @N(5) value: Bytes
  )

final case class BucketInfo
  ( @N(1) vc: VectorClock
  , @N(2) keys: Vector[Bytes]
  )

object codec {
  implicit val bucketInfoCodec: MessageCodec[BucketInfo] = caseCodecAuto[BucketInfo]
  implicit val dataCodec: MessageCodec[Data] = caseCodecAuto[Data]
}
