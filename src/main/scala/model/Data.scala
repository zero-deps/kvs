package mws.rng.data

import zd.proto.api.{N, MessageCodec}
import zd.proto.macrosapi.messageCodecAuto


final case class Data
  ( @N(1) key: Array[Byte]
  , @N(2) bucket: Int
  , @N(3) lastModified: Long
  , @N(4) vc: Vector[Vec]
  , @N(5) value: Array[Byte]
  )

final case class Vec
  ( @N(1) key: String
  , @N(2) value: Long
  )

final case class BucketInfo
  ( @N(1) vc: Vector[Vec]
  , @N(2) keys: Vector[Array[Byte]]
  )

object codec {
  implicit val bucketInfoCodec: MessageCodec[BucketInfo] = messageCodecAuto[BucketInfo]
  implicit val vecCodec: MessageCodec[Vec] = messageCodecAuto[Vec]
  implicit val dataCodec: MessageCodec[Data] = messageCodecAuto[Data]
}
