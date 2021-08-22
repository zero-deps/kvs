package kvs.seq

import proto.*

case class Named[A](@N(1) _id: Array[Byte], @N(2) a: A)
object Named {
  implicit def namedC[A: MessageCodec]: MessageCodec[Named[A]] = caseCodecAuto[Named[A]]
}
