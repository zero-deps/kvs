package kvs.seq

import zd.proto.api.{N, MessageCodec}
import zd.proto.macrosapi.caseCodecAuto

case class Named[A](@N(1) name: String, @N(2) a: A)
object Named {
  implicit def namedC[A: MessageCodec]: MessageCodec[Named[A]] = caseCodecAuto[Named[A]]
}
