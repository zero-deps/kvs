package zd.kvs
package rng.dump

import zd.proto.api.{MessageCodec, N}
import zd.proto.macrosapi.{caseCodecAuto, classCodecAuto}

final case class DumpKV
  ( @N(1) kv: Vector[KV]
  )

final case class KV
  ( @N(1) k: Bytes
  , @N(2) v: Bytes
  )

object codec {
  implicit val dumpKVCodec: MessageCodec[DumpKV] = caseCodecAuto[DumpKV]
  implicit val kVCodec: MessageCodec[KV] = classCodecAuto[KV]
}
