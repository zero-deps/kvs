package mws

import akka.actor.Address
import akka.cluster.VectorClock

/**
 */
package object rng {
  type Bucket = Int
  type VNode = Int
  type Node = Address
  type Key = String
  type Value = String // todo:ByteString

  type KaiBucket = (Bucket, List[Node])
  type Replica = Option[Int]

  type HashBucket = (Int, Replica, Replica)

  type Data = (
    Key, 
    Bucket,
    Long,     // last_modified
    VectorClock,
    String,   // checksum
    String,   // flags
    Value)

  case object ChooseNodeRandomly
  case object ChooseBuckerRandomly
}
