package kvs
package rng
package model

import zd.proto.api.N
import zd.proto.Bytes

import rng.data._

sealed trait Msg

@N(1) final case class ChangeState
  ( @N(1) s: QuorumState
  ) extends Msg

@N(2) final case class DumpBucketData
  ( @N(1) b: Int
  , @N(2) items: Vector[KeyBucketData]
  ) extends Msg

@N(5) final case class DumpGetBucketData 
  ( @N(1) b: Int
  ) extends Msg

@N(7) final case class ReplBucketPut
  ( @N(1) b: Int
  , @N(2) bucketVc: VectorClock
  , @N(3) items: Vector[KeyBucketData]
  ) extends Msg

@N(8) final case object ReplBucketUpToDate extends Msg

@N(9) final case class ReplGetBucketIfNew
  ( @N(1) b: Int
  , @N(2) vc: VectorClock
  ) extends Msg

@N(10) final case class ReplNewerBucketData
  ( @N(1) vc: VectorClock
  , @N(2) items: Vector[KeyBucketData]
  ) extends Msg

final case class ReplBucketsVc
  ( @N(1) bvcs: Map[Int, VectorClock]
  )

@N(11) final case class StoreDelete
  ( @N(1) key: Bytes
  ) extends Msg

@N(12) final case class StoreGet
  ( @N(1) key: Bytes
  ) extends Msg

@N(13) final case class StoreGetAck
  ( @N(1) key: Bytes
  , @N(2) bucket: Int
  , @N(3) data: Option[Data]
  ) extends Msg

@N(14) final case class StorePut
  ( @N(1) key: Bytes
  , @N(2) bucket: Int
  , @N(3) data: Data
  ) extends Msg

final case class KeyBucketData
  ( @N(1) key: Bytes
  , @N(2) bucket: Int
  , @N(3) data: Data
  )

sealed trait QuorumState
object QuorumState {
  @N(1) final case object QuorumStateUnsatisfied extends QuorumState
  @N(2) final case object QuorumStateReadonly extends QuorumState
  @N(3) final case object QuorumStateEffective extends QuorumState
}
