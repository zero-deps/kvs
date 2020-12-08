package kvs.seq

import zd.proto._, api._, macrosapi._

case class Named[A](@N(1) _id: Bytes, @N(2) a: A)
object Named {
  implicit def namedC[A: MessageCodec]: MessageCodec[Named[A]] = caseCodecAuto[Named[A]]
}
