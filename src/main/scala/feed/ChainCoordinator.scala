package feed

import java.util.concurrent.TimeUnit

import akka.actor._
import akka.cluster.ClusterEvent.{CurrentClusterState, MemberRemoved, MemberUp}
import akka.cluster.{Member, ClusterEvent, Cluster}
import mws.rng._
import org.iq80.leveldb.DB
import scala.collection.{SortedMap, breakOut}
import scala.concurrent.duration.FiniteDuration

trait ChainState
case object Startup extends ChainState
case object Running extends ChainState

case class CoordinatorData(vNodes: SortedMap[Int, Node],
                            chains: Map[Int, PreferenceList])

class ChainCoordinator(db: DB) extends FSM[ChainState, CoordinatorData] with ActorLogging{
  startWith(Startup, CoordinatorData(SortedMap.empty[Int,Node], Map.empty[Int, PreferenceList]))

  val system = context.system
  val hashing = HashingExtension(system)
  val chainSize = system.settings.config.getInt("ring.chain.length")
  val chainNumber = system.settings.config.getInt("ring.chain.number")
  val selection = SelectionMemorize(system)

  val cluster = Cluster(system)
  val vnode = system.settings.config.getInt("ring.virtual-nodes")

  override def preStart() = {
    cluster.subscribe(self, ClusterEvent.InitialStateAsSnapshot, classOf[MemberUp], classOf[MemberRemoved])
    log.info(s"chain coordinator is started $self")
  }


  when(Startup,stateTimeout = FiniteDuration (1, TimeUnit.SECONDS)){
    case Event(StateTimeout, _) =>
      log.info(s"22222222222")
      stay()

    case Event(s:String, _) =>
      log.info(s"Startup  $s")
      stay()
    case Event(state:CurrentClusterState, _) =>
      if(state.members.isEmpty){
        cluster.sendCurrentClusterState(self)
        stay()
      }else {
        log.info(s"[coordinator] not empty state, ${state.members}")
//        val vNodes: SortedMap[VNode, Node] = state.members.foldLeft(SortedMap.empty[VNode, Address])((acc, member) =>
//          acc ++ virtualNodes(member))
//
//        log.info(s"[++++++] virtual nodes = $vNodes")
//
//        val chains: Map[Int, PreferenceList] = (0 to chainNumber).foldLeft(Map.empty[Bucket, PreferenceList])((acc, chainID) =>
//          acc + (chainID -> hashing.findNodes(chainID * hashing.chainRange, vNodes, state.members.size)))
//        log.info(s"[++++++] chains $chains ")
//        //spawn chains servers for each chain ID
//        (0 to chainNumber).foreach(id => context.child(s"chain-server-$id").getOrElse(context.actorOf(Props(classOf[ChainServer], db), s"chain-server-$id")))
        goto(Running) //using CoordinatorData(vNodes, chains)
      }
    case Event(add@Add(fid,v), data) =>
      val chainID = hashing.findChain(fid)
      log.info(s"[coordinator] Start add: $add, chainID = $chainID")
      data.chains(chainID) match {
        case chain if chain.head == self.path.address => context.child(s"chain-server-$chainID").foreach(_.tell(add, sender()))
        case foreign => selection.get(foreign.head, "coordinator").fold(_.tell(Add(fid,v), sender()), _.tell(add, sender()))
      }
      stay()
  }

  when(Running ,stateTimeout = FiniteDuration (1, TimeUnit.SECONDS) ) {
    case Event(s:String, _) =>
      log.info(s"Running  $s")
      stay()
    case Event(StateTimeout, _) =>
      log.info(s"!!!!!!!!!!!!!!!!")
      stay()
    case Event(add@Add(fid,v), data) =>
      val chainID = hashing.findChain(fid)
      log.info(s"[coordinator] add: $add, chainID = $chainID")
      data.chains(chainID) match {
        case chain if chain.head == self.path.address => context.child(s"chain-server-$chainID").foreach(_.tell(add, sender()))
        case foreign => selection.get(foreign.head, "coordinator").fold(_.tell(Add(fid,v), sender()), _.tell(add, sender()))
      }
      stay()
    case Event(rm@Remove(fid,_), data) =>
      log.info(s"[coordinator] rm: $rm")
      val chainID = hashing.findChain(fid)
      data.chains(chainID) match {
        case chain if chain.head == self.path.address => context.child(s"chain-server-$chainID").foreach(_.tell(rm, sender()))
        case foreign => selection.get(foreign.head, "coordinator").fold(_.tell(rm, sender()), _.tell(rm, sender()))
      }
      stay()
    case Event(t@Traverse(fid,_,_), data) =>
      log.info(s"[coordinator] travers: $t")
      val chainID = hashing.findChain(fid)
      data.chains(chainID) match {
        case chain if chain.last == self.path.address => context.child(s"chain-server-$chainID").foreach(_.tell(t, sender()))
        case foreign => selection.get(foreign.head, "coordinator").fold(_.tell(t, sender()), _.tell(t, sender()))
      }
      stay()
      // membership
    case Event(MemberUp(m), CoordinatorData(vNodes, chains)) =>
      log.info(s"[coordinator], $m added to chain servers")
//      val newvNodes= vNodes ++ virtualNodes(m)
//      val chainUpdated = (0 to chainNumber).map(chainID => chainID -> hashing.findNodes(chainID * hashing.chainRange, newvNodes, chainSize))
//      val synch: Map[Int, PreferenceList] = (chainUpdated.toSet diff chains.toSet).toMap
//      //TODO SYNCH chains
      stay() // using CoordinatorData(newvNodes, chainUpdated.map{case (fid, nodes) => fid -> nodes}.toMap)
    case Event(MemberRemoved, data) =>
      stay()
  }

//  whenUnhandled {
//    case Event(e, s) =>
//      log.info("received unhandled request {} in state {}/{}", e, stateName, s)
//      stay
//  }

  def virtualNodes(m:Member): SortedMap[VNode, Address] = {
    (1 to vnode).map(vnode => {
      hashing.hash(m.address.hostPort + vnode) -> m.address
    })(breakOut)
  }
}