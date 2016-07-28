package feed

import akka.actor.{Address, FSM, ActorLogging}
import akka.cluster.ClusterEvent.{CurrentClusterState, MemberRemoved, MemberUp}
import akka.cluster.{Member, ClusterEvent, Cluster}
import mws.rng._
import scala.collection.{SortedMap, breakOut}

trait ChainState
case object Startup extends ChainState
case object Running extends ChainState
case object Partitioning extends ChainState
case class CoordinatorData(vNodes: SortedMap[Int, Node],
                            chains: Map[Int, PreferenceList])

class ChainCoordinator extends FSM[ChainState, CoordinatorData] with ActorLogging{
  startWith(Startup, CoordinatorData(SortedMap.empty[Int,Node], Map.empty[Int, PreferenceList]))

  val system = context.system
  val hashing = HashingExtension(system)
  val chainSize = system.settings.config.getInt("ring.chain.length")
  val chainNumber = system.settings.config.getInt("ring.chain.number")

  val cluster = Cluster(system)
  val vnode = system.settings.config.getInt("ring.virtual-nodes")

  override def preStart() = {
    cluster.subscribe(self, ClusterEvent.initialStateAsEvents, classOf[MemberUp], classOf[MemberRemoved])
  }

  when(Startup){
    case Event(state:CurrentClusterState, _) =>
      val virtualNodes: SortedMap[VNode, Node] = state.members.foldLeft(SortedMap.empty[VNode, Address])((acc, member) =>
        acc ++ updateVirtualNodes(member))

      val chains: Map[Int, PreferenceList] = (0 to chainNumber).foldLeft(Map.empty[Bucket, PreferenceList])((acc, chainID) =>
        acc + (chainID -> hashing.findNodes(chainID * hashing.chainRange , virtualNodes, state.members.size)))

      goto(Running) using CoordinatorData(virtualNodes, chains)
  }

  when(Running){
    case Event(MemberUp(m), CoordinatorData(vNodes, chains)) =>
      log.info(s"[feed], $m added to chain servers")
      val newvNodes= vNodes ++ updateVirtualNodes(m)

      val chainUpdated = (0 to chainNumber).map(chainID => chainID -> hashing.findNodes(chainID * hashing.chainRange, newvNodes, chainSize))

      val synch: Map[Int, PreferenceList] = (chainUpdated.toSet diff chains.toSet).toMap

      stay() // using CoordinatorData(updvNodes, chainUpdated.map{case (fid, nodes) => fid -> nodes}.toMap)

    case Event(MemberRemoved, data) =>
      stay()
    case Event(add@Add(fid,_), data) =>
      data.chains.get(hashing.findChain(fid)) match
      { 
        case None => sender() ! Right("invalid_fid")
        case Some(ch) =>  //ch.head.tell(add, sender())
      }
      stay()
  }

  def updateVirtualNodes(m:Member): SortedMap[VNode, Address] = {
    (1 to vnode).map(vnode => {
      hashing.hash(m.address.hostPort + vnode) -> m.address
    })(breakOut)
  }
}