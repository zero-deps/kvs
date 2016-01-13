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
  type FeedId = String
  type FeedBucket = Bucket

  type PreferenceList = List[Node]
  type ReplicaKey = Option[Int]

  type SynchReplica = (Bucket, ReplicaKey, ReplicaKey)
  
  //TODO try lm from VectorClock.versions: TreeMap[VectorClock.Node, Long]
  case class Data(key: Key, bucket: Bucket, lastModified: Long, vc: VectorClock, value: Value)
  case class Feed(fid: Key, lastModified: Long, vc: VectorClock, value: List[Value])
  
  //FSM
  sealed trait FsmState
  case object ReadyCollect extends FsmState
  case object Collecting extends FsmState
  case object Sent extends FsmState

  sealed trait FsmData
  case class Statuses(all: List[PutStatus]) extends FsmData
  /**
   * inconsistent means that key points to more then one values. So data is inconsistent. 
   * It's not related to vector clock.
   * */
  case class DataCollection(perNode: List[(Option[Data], Node)], inconsistent: List[(List[Data], Node)]) extends FsmData{
    def size = perNode.size + inconsistent.size
  }
  case class ReceivedValues(n: Int) extends FsmData
  case object OpsTimeout

  def orderHistorically(l: List[(Node, VectorClock)]) :
  (List[(Node, VectorClock)],List[(Node, VectorClock)],List[(Node, VectorClock)]) = ???

}
