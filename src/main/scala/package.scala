package mws

import akka.actor.Address
import akka.cluster.VectorClock
import akka.util.ByteString
import mws.rng.store.PutStatus

import scala.annotation.tailrec

package object rng {
  type Bucket = Int
  type VNode = Int
  type Node = Address
  type Key = String
  type Value = ByteString
  type NamedBucketId = String
  type FeedBucket = Bucket
  type Age = (VectorClock, Long)
  type PreferenceList = Set[Node]
  
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
  case class DataCollection(perNode: List[(Option[Data], Node)], nodes: Int) extends FsmData
  case object OpsTimeout

  /* returns (actual data, list of outdated nodes) */
  def order[E](l: List[E], age: E => Age): (Option[E], List[E]) = {
    @tailrec
    def itr(l: List[E], newest: E): E = l match {
      case Nil => newest
      case h :: t if t.exists(age(h)._1 < age(_)._1) => itr(t, newest )
      case h :: t if age(h)._1 > age(newest)._1 => itr(t, h)
      case h :: t if age(h)._1 <> age(newest)._1 &&
        age(h)._2 > age(newest)._2 => itr(t, h)
      case _ => itr(l.tail, newest)
    }

    (l map (age(_)._1)).toSet.size match {
      case 0 => (None, Nil)
      case 1 => (Some(l.head), Nil)
      case n =>
        val correct = itr(l.tail, l.head)
        (Some(correct), l.filterNot(age(_)._1 == age(correct)._1))
    }
  }

  @tailrec
  def mergeBucketData(l: List[Data], merged: List[Data]): List[Data] = l match {
    case h :: t =>
      merged.find(_.key == h.key) match {
        case Some(d) if h.vc == d.vc && h.lastModified > d.lastModified =>
          mergeBucketData(t, h :: merged.filterNot(_.key == h.key))
        case Some(d) if h.vc > d.vc =>
          mergeBucketData(t, h :: merged.filterNot(_.key == h.key))
        case None => mergeBucketData(t, h :: merged)
        case _ => mergeBucketData(t, merged)
      }
    case Nil => merged
  }
}
