package zd.rng
package data

import proto.*
import java.util.Arrays

final class Data
  ( @N(1) val key: Array[Byte]
  , @N(2) val bucket: Int
  , @N(3) val lastModified: Long
  , @N(4) val vc: VectorClock
  , @N(5) val value: Array[Byte]
  ) {
  def copy(vc: VectorClock): Data = {
    new Data(key=key, bucket=bucket, lastModified=lastModified, vc=vc, value=value)
  }
  override def equals(other: Any): Boolean = other match {
    case that: Data =>
      Arrays.equals(key, that.key) &&
      bucket == that.bucket &&
      lastModified == that.lastModified &&
      vc == that.vc &&
      Arrays.equals(value, that.value)
    case _ => false
  }
  override def hashCode(): Int = {
    val state = Seq[Any](key, bucket, lastModified, vc, value)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
  override def toString = s"Data(key=$key, bucket=$bucket, lastModified=$lastModified, vc=$vc, value=$value)"
}
object Data {
  def apply(key: Array[Byte], bucket: Int, lastModified: Long, vc: VectorClock, value: Array[Byte]): Data = {
    new Data(key=key, bucket=bucket, lastModified=lastModified, vc=vc, value=value)
  }
}

final case class BucketInfo
  ( @N(1) vc: VectorClock
  , @N(2) keys: Vector[Array[Byte]]
  )

object codec {
  import akka.cluster.given
  implicit val bucketInfoCodec: MessageCodec[BucketInfo] = caseCodecAuto[BucketInfo]
  implicit val dataCodec: MessageCodec[Data] = classCodecAuto[Data]
}
