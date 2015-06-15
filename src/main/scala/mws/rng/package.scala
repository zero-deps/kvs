package mws

import akka.actor.Address
import akka.cluster.VectorClock

package object rng {
  type Bucket = Int
  type VNode = Int
  type Node = Address
  type Key = String
  type Value = String // todo:ByteString

  type RingBucket = (Bucket, List[Node])
  type ReplicaKey = Option[Int]

  type HashBucket = (Int, ReplicaKey, ReplicaKey)
  
  //TODO try lm from VectorClock.versions: TreeMap[VectorClock.Node, Long]
  case class Data(key: Key, bucket: Bucket, lastModified: Long, vc: VectorClock, value: Value)
}
