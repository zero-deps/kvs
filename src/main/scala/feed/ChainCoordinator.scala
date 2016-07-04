package feed

import akka.actor.FSM
import akka.cluster.ClusterEvent.{MemberRemoved, MemberUp}
import akka.cluster.{ClusterEvent, Cluster}
import mws.rng.{Node, HashingExtension, Value}

import scala.collection.immutable.SortedMap

case class Add(fid: FID, v: Value)
case class Traverse(fid: String, start: Option[String], count: Option[Int])
case class Remove(nb: String, v: Value)
case class RegisterFeed(fid: String)

trait CRState
case object Running extends CRState
case class CoordinatorData(
      vNodes: SortedMap[Int, Node],
      fchains: Map[FID, Chain]) // mutable state, hashOfNode->Node

class ChainCoordinator extends FSM[CRState, CoordinatorData]{
  startWith(Running, CoordinatorData(SortedMap.empty[Int,Node], Map.empty[FID,Chain]))

  val system = context.system
  val hashing = HashingExtension(system)
  val chainSize = system.settings.config.getIntList("ring.quorum").get(0)
  val cluster = Cluster(system)

  override def preStart() = {
    cluster.subscribe(self, ClusterEvent.initialStateAsEvents, classOf[MemberUp], classOf[MemberRemoved])
  }

  when(Running){
    case Event(MemberUp, data) =>

      stay()
    case Event(MemberRemoved, data) =>
      stay()
    case Event(add@Add(fid,_), data) =>
      data.fchains.get(fid) match
      { 
        case None => sender() ! Right("invalid_fid")
        case Some(ch) =>  ch.head.tell(add, sender())
      }
      stay()
  }

}
