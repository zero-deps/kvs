package mws

import akka.actor.Address
import akka.cluster.VectorClock
import akka.util.ByteString

package object rng {
  type Bucket = Int
  type VNode = Int
  type Node = Address
  type Key = String
  type Value = ByteString

  type PreferenceList = List[Node]
  type ReplicaKey = Option[Int]

  type SynchReplica = (Bucket, ReplicaKey, ReplicaKey)
  
  //TODO try lm from VectorClock.versions: TreeMap[VectorClock.Node, Long]
  case class Data(key: Key, bucket: Bucket, lastModified: Long, vc: VectorClock, value: Value)
}
