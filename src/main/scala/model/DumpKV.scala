package mws.rng.dump

import zd.proto.api.{MessageCodec, N}
import zd.proto.macrosapi.messageCodecAuto

final case class DumpKV
  ( @N(1) kv: Vector[KV]
  )

final case class KV
  ( @N(1) k: Array[Byte]
  , @N(2) v: Array[Byte]
  )

final case class ValueKey
  ( @N(1) v: Array[Byte]
  , @N(2) nextKey: Array[Byte]
  )

object codec {
  implicit val dumpKVCodec: MessageCodec[DumpKV] = messageCodecAuto[DumpKV]
  implicit val kVCodec: MessageCodec[KV] = messageCodecAuto[KV]
  implicit val valueKeyCodec: MessageCodec[ValueKey] = messageCodecAuto[ValueKey]
}
