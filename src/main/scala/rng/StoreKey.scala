package zd.kvs
package rng
package store

import zd.proto.api.{N, MessageCodec}
import zd.proto.macrosapi.{caseCodecAuto, sealedTraitCodecAuto}

sealed trait StoreKey
@N(1) final case class BucketInfoKey(@N(1) bucket: Int) extends StoreKey
@N(2) final case class DataKey(@N(1) bucket: Int, @N(2) key: Bytes) extends StoreKey

object codec {
  implicit val StoreKeyC: MessageCodec[StoreKey] = {
    implicit val BucketInfoKeyC: MessageCodec[BucketInfoKey] = caseCodecAuto[BucketInfoKey]
    implicit val DataKeyC: MessageCodec[DataKey] = caseCodecAuto[DataKey]
    sealedTraitCodecAuto[StoreKey]
  }
}