package zd.rng
package model

import zd.proto.api.N
import zd.rng.data._

sealed trait Msg

@N(1) final case class ChangeState
  ( @N(1) s: QuorumState
  ) extends Msg

@N(2) final case class DumpBucketData
  ( @N(1) b: Int
  , @N(2) items: Vector[Data]
  ) extends Msg

@N(3) final case class DumpEn
  ( @N(1) k: Array[Byte]
  , @N(2) v: Array[Byte]
  , @N(3) nextKey: Array[Byte]
  ) extends Msg

@N(4) final case class DumpGet
  ( @N(1) k: Array[Byte]
  ) extends Msg

@N(5) final case class DumpGetBucketData 
  ( @N(1) b: Int
  ) extends Msg

@N(6) final case class DumpPut
  ( @N(1) k: Array[Byte]
  , @N(2) v: Array[Byte]
  , @N(3) prev: Array[Byte]
  ) extends Msg

@N(7) final case class ReplBucketPut
  ( @N(1) b: Int
  , @N(2) bucketVc: VectorClock
  , @N(3) items: Vector[Data]
  ) extends Msg

@N(8) final case object ReplBucketUpToDate extends Msg

@N(9) final case class ReplGetBucketIfNew
  ( @N(1) b: Int
  , @N(2) vc: VectorClock
  ) extends Msg

@N(10) final case class ReplNewerBucketData
  ( @N(1) vc: VectorClock
  , @N(2) items: Vector[Data]
  ) extends Msg

final case class ReplBucketsVc
  ( @N(1) bvcs: Map[Int, VectorClock]
  )

@N(11) final case class StoreDelete
  ( @N(1) key: Array[Byte] 
  ) extends Msg

@N(12) final case class StoreGet
  ( @N(1) key: Array[Byte]
  ) extends Msg

@N(13) final case class StoreGetAck
  ( @N(1) data: Option[Data]
  ) extends Msg

@N(14) final case class StorePut
  ( @N(1) data: Data
  ) extends Msg

final case class ReplGetBucketsVc
  ( @N(1) bs: Vector[Int]
  )

sealed trait QuorumState
object QuorumState {
  @N(1) final case object QuorumStateUnsatisfied extends QuorumState
  @N(2) final case object QuorumStateReadonly extends QuorumState
  @N(3) final case object QuorumStateEffective extends QuorumState
}
