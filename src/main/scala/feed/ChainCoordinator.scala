package feed

import akka.actor.{FSM, ActorLogging}
import akka.cluster.ClusterEvent.{MemberRemoved, MemberUp}
import akka.cluster.{ClusterEvent, Cluster}
import mws.rng.{VNode, Node, HashingExtension, Value}
import scala.collection.immutable.SortedMap


trait CRState
case object Running extends CRState
case class CoordinatorData(
      vNodes: SortedMap[Int, Node],
      fchains: Map[FID, Chain])

class ChainCoordinator extends FSM[CRState, CoordinatorData] with ActorLogging{
  startWith(Running, CoordinatorData(SortedMap.empty[Int,Node], Map.empty[FID,Chain]))

  val system = context.system
  val hashing = HashingExtension(system)
  val chainSize = system.settings.config.getIntList("ring.quorum").get(0) //TODO new conf. prop.?
  val cluster = Cluster(system)
  val vnode = system.settings.config.getInt("ring.virtual-nodes")

  override def preStart() = {
    cluster.subscribe(self, ClusterEvent.initialStateAsEvents, classOf[MemberUp], classOf[MemberRemoved])
  }

  when(Running){
    case Event(MemberUp(m), data) =>
      log.info(s"[feed], $m added to chain servers")
       val newvNodes: Map[VNode, Node] = (1 to vnode).map(vnode => {
         hashing.hash(m.address.hostPort + vnode) -> m.address})(breakOut)
       val updvNodes = data.vNodes ++ newvNodes

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