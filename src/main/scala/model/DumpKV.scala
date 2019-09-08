package zd.kvs
package rng.dump

import zd.proto.api.{MessageCodec, N}
import zd.proto.macrosapi.{caseCodecAuto, classCodecAuto}
import java.util.Arrays

final case class DumpKV
  ( @N(1) kv: Vector[KV]
  )

final class KV
  ( @N(1) val k: Array[Byte]
  , @N(2) val v: Array[Byte]
  ) {
  override def equals(other: Any): Boolean = other match {
    case that: KV =>
      Arrays.equals(k, that.k) &&
      Arrays.equals(v, that.v)
    case _ => false
  }
  override def hashCode(): Int = {
    val state = Seq(k, v)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
  override def toString = s"KV(k=$k, v=$v)"
}

object KV {
  def apply(k: Array[Byte], v: Array[Byte]): KV = {
    new KV(k=k, v=v)
  }
}

object codec {
  implicit val dumpKVCodec: MessageCodec[DumpKV] = caseCodecAuto[DumpKV]
  implicit val kVCodec: MessageCodec[KV] = classCodecAuto[KV]
}
