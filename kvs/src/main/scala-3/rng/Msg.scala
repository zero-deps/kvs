package zd.rng
package model

import proto.*
import zd.rng.data.*
import java.util.Arrays

sealed trait Msg

@N(1) final case class ChangeState
  ( @N(1) s: QuorumState
  ) extends Msg

@N(2) final case class DumpBucketData
  ( @N(1) b: Int
  , @N(2) items: Vector[Data]
  ) extends Msg

@N(3) final class DumpEn
  ( @N(1) val k: Array[Byte]
  , @N(2) val v: Array[Byte]
  , @N(3) val nextKey: Array[Byte]
  ) extends Msg {
  override def equals(other: Any): Boolean = other match {
    case that: DumpEn =>
      Arrays.equals(k, that.k) &&
      Arrays.equals(v, that.v) &&
      Arrays.equals(nextKey, that.nextKey)
    case _ => false
  }
  override def hashCode(): Int = {
    val state = Seq(k, v, nextKey)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
  override def toString = s"DumpEn(k=$k, v=$v, nextKey=$nextKey)"
}

object DumpEn {
  def apply(k: Array[Byte], v: Array[Byte], nextKey: Array[Byte]): DumpEn = {
    new DumpEn(k=k, v=v, nextKey=nextKey)
  }
}

@N(4) final class DumpGet
  ( @N(1) val k: Array[Byte]
  ) extends Msg {
  override def equals(other: Any): Boolean = other match {
    case that: DumpGet =>
      Arrays.equals(k, that.k)
    case _ => false
  }
  override def hashCode(): Int = {
    val state = Seq(k)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
  override def toString = s"DumpGet(k=$k)"
}

object DumpGet {
  def apply(k: Array[Byte]): DumpGet = {
    new DumpGet(k=k)
  }
}

@N(5) final case class DumpGetBucketData 
  ( @N(1) b: Int
  ) extends Msg

@N(7) final case class ReplBucketPut
  ( @N(1) b: Int
  , @N(2) bucketVc: VectorClock
  , @N(3) items: Vector[Data]
  ) extends Msg

@N(8) case object ReplBucketUpToDate extends Msg

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

@N(11) final class StoreDelete
  ( @N(1) val key: Array[Byte] 
  ) extends Msg {
  override def equals(other: Any): Boolean = other match {
    case that: StoreDelete =>
      Arrays.equals(key, that.key)
    case _ => false
  }
  override def hashCode(): Int = {
    val state = Seq(key)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
  override def toString = s"StoreDelete(key=$key)"
}

object StoreDelete {
  def apply(key: Array[Byte]): StoreDelete = {
    new StoreDelete(key=key)
  }
}

@N(12) final class StoreGet
  ( @N(1) val key: Array[Byte]
  ) extends Msg {
  override def equals(other: Any): Boolean = other match {
    case that: StoreGet =>
      Arrays.equals(key, that.key)
    case _ => false
  }
  override def hashCode(): Int = {
    val state = Seq(key)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
  override def toString = s"StoreGet(key=$key)"
}

object StoreGet {
  def apply(key: Array[Byte]): StoreGet = {
    new StoreGet(key=key)
  }
}

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
  @N(1) case object QuorumStateUnsatisfied extends QuorumState
  @N(2) case object QuorumStateReadonly extends QuorumState
  @N(3) case object QuorumStateEffective extends QuorumState
}
