// package mws

// import scodec.bits.BitVector
// import scodec.codecs._
// import scodec.{Attempt, DecodeResult, Err => CErr, Codec, SizeBound}

// object ScodecBuilder {
//   def option[A](codecA: Codec[A]) = new Codec[Option[A]] {
//     def sizeBound = SizeBound.unknown
//     val codec = list(codecA)
//     def encode(a: Option[A]) = codec.encode(a.toList)
//     def decode(bytes: BitVector) = codec.decode(bytes).map(_.map(_.headOption))
//   }
//   val abytes = new Codec[Array[Byte]] {
//     def sizeBound = SizeBound.unknown
//     def encode(a: Array[Byte]) = Attempt.successful(BitVector.view(a))
//     def decode(bytes: BitVector) = Attempt.successful(DecodeResult(bytes.toByteArray, BitVector.empty))
//     def toString = "byteArray"
//   }
//   def caseCodec1[A, Z](apply: (A) => Z, unapply: Z => Option[A])(codecA: Codec[A]): Codec[Z] = new Codec[Z] {
//     def sizeBound = SizeBound.unknown
//     val codec = codecA
//     def encode(z: Z) = {
//       unapply(z).fold[Attempt[BitVector]](Attempt.failure(CErr("failed to unapply")))(codec.encode)
//     }
//     def decode(bytes: BitVector) = codec.decode(bytes).map(_.map{ case a => apply(a) })
//   }
//   def caseCodec2[A, B, Z](apply: (A, B) => Z, unapply: Z => Option[(A, B)])(codecA: Codec[A], codecB: Codec[B]): Codec[Z] = new Codec[Z] {
//     def sizeBound = SizeBound.unknown
//     val codec = codecA ~ codecB
//     def encode(z: Z) = unapply(z).fold[Attempt[BitVector]](Attempt.failure(CErr("failed to unapply")))(t => codec.encode(t._1 ~ t._2))
//     def decode(bytes: BitVector) = codec.decode(bytes).map(_.map{ case (a, b) => apply(a, b) })
//   }
//   def caseCodec3[A, B, C, Z](apply: (A, B, C) => Z, unapply: Z => Option[(A, B, C)])(codecA: Codec[A], codecB: Codec[B], codecC: Codec[C]): Codec[Z] = new Codec[Z] {
//     def sizeBound = SizeBound.unknown
//     val codec = codecA ~ codecB ~ codecC
//     def encode(z: Z) = unapply(z).fold[Attempt[BitVector]](Attempt.failure(CErr("failed to unapply")))(t => codec.encode(t._1 ~ t._2 ~ t._3))
//     def decode(bytes: BitVector) = codec.decode(bytes).map(_.map{ case ((a, b), c) => apply(a, b, c) })
//   }
//   def caseCodec5[A, B, C, D, E, Z](apply: (A, B, C, D, E) => Z, unapply: Z => Option[(A, B, C, D, E)])(codecA: Codec[A], codecB: Codec[B], codecC: Codec[C], codecD: Codec[D], codecE: Codec[E]): Codec[Z] = new Codec[Z] {
//     def sizeBound = SizeBound.unknown
//     val codec = codecA ~ codecB ~ codecC ~ codecD ~ codecE
//     def encode(z: Z) = unapply(z).fold[Attempt[BitVector]](Attempt.failure(CErr("failed to unapply")))(t => codec.encode(t._1 ~ t._2 ~ t._3 ~ t._4 ~ t._5))
//     def decode(bytes: BitVector) = codec.decode(bytes).map(_.map{ case ((((a, b), c), d), e) => apply(a, b, c, d, e) })
//   }
//   def typed[A](n: Int)(codecA: Codec[A]): Codec[A] = new Codec[A] {
//     def sizeBound = SizeBound.unknown
//     val codec = byte ~ codecA
//     def encode(a: A) = codec.encode(n.toByte ~ a)
//     def decode(bytes: BitVector) = codec.decode(bytes).map(_.map{ case (`n`, a) => a })
//   }
// }
