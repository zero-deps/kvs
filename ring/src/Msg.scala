package kvs.rng
package model

import proto.N

import kvs.rng.data.*

sealed trait Msg

@N(1) case class ChangeState
  ( @N(1) s: QuorumState
  ) extends Msg

@N(2) case class DumpBucketData
  ( @N(1) b: Int
  , @N(2) items: Vector[KeyBucketData]
  ) extends Msg

@N(5) case class DumpGetBucketData 
  ( @N(1) b: Int
  ) extends Msg

@N(7) case class ReplBucketPut
  ( @N(1) b: Int
  , @N(2) bucketVc: VectorClock
  , @N(3) items: Vector[KeyBucketData]
  ) extends Msg

@N(8) case object ReplBucketUpToDate extends Msg

@N(9) case class ReplGetBucketIfNew
  ( @N(1) b: Int
  , @N(2) vc: VectorClock
  ) extends Msg

@N(10) case class ReplNewerBucketData
  ( @N(1) vc: VectorClock
  , @N(2) items: Vector[KeyBucketData]
  ) extends Msg

case class ReplBucketsVc
  ( @N(1) bvcs: Map[Int, VectorClock]
  )

@N(11) case class StoreDelete
  ( @N(1) key: Array[Byte]
  ) extends Msg

@N(12) case class StoreGet
  ( @N(1) key: Array[Byte]
  ) extends Msg

@N(13) case class StoreGetAck
  ( @N(1) key: Array[Byte]
  , @N(2) bucket: Int
  , @N(3) data: Option[Data]
  ) extends Msg

@N(14) case class StorePut
  ( @N(1) key: Array[Byte]
  , @N(2) bucket: Int
  , @N(3) data: Data
  ) extends Msg

case class KeyBucketData
  ( @N(1) key: Array[Byte]
  , @N(2) bucket: Int
  , @N(3) data: Data
  )

sealed trait QuorumState
object QuorumState {
  @N(1) case object QuorumStateUnsatisfied extends QuorumState
  @N(2) case object QuorumStateReadonly extends QuorumState
  @N(3) case object QuorumStateEffective extends QuorumState
}
