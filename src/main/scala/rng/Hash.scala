package mws.rng

import akka.actor._
import akka.cluster.ClusterEvent._
import akka.cluster.{Member, Cluster}
import akka.util.Timeout
import com.typesafe.config.Config
import mws.rng.msg.{StoreDelete, StoreGet, QuorumState, QuorumStateUnsatisfied, QuorumStateReadonly, QuorumStateEffective, ChangeState}
import scala.collection.immutable.{SortedMap, SortedSet}
import scala.collection.{breakOut}
import scala.concurrent.duration._
import scalaz.Scalaz._

object Hash {
  def props(): Props = Props(new Hash)
}

// TODO available/not avaiable nodes
class Hash extends FSM[QuorumState, HashRngData] with ActorLogging {
  import context.system
  implicit val timeout = Timeout(5 seconds)

  val config: Config = system.settings.config.getConfig("ring")

  val quorum = config.getIntList("quorum")
  val N: Int = quorum.get(0)
  val W: Int = quorum.get(1)
  val R: Int = quorum.get(2)
  val gatherTimeout = Duration.fromNanos(config.getDuration("gather-timeout").toNanos)
  val vNodesNum = config.getInt("virtual-nodes")
  val bucketsNum = config.getInt("buckets")
  val cluster = Cluster(system)
  val local: Node = cluster.selfAddress
  val hashing = HashingExtension(system)
  val actorsMem = SelectionMemorize(system)

  log.info(s"Ring configuration:".blue)
  log.info(s"ring.quorum.N = ${N}".blue)
  log.info(s"ring.quorum.W = ${W}".blue)
  log.info(s"ring.quorum.R = ${R}".blue)
  log.info(s"ring.leveldb.dir = ${config.getString("leveldb.dir")}".blue)

  startWith(QuorumStateUnsatisfied(), HashRngData(Set.empty[Node], SortedMap.empty[Bucket, PreferenceList], SortedMap.empty[Bucket, Node], replication=None))

  override def preStart() = {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents, classOf[MemberUp], classOf[MemberRemoved])
  }
  
  override def postStop(): Unit = cluster.unsubscribe(self)

  when(QuorumStateUnsatisfied()){
    case Event(Get(_), _) =>
      sender ! AckQuorumFailed("QuorumStateUnsatisfied")
      stay()
    case Event(Put(_,_), _) =>
      sender ! AckQuorumFailed("QuorumStateUnsatisfied")
      stay()
    case Event(Delete(_), _) =>
      sender ! AckQuorumFailed("QuorumStateUnsatisfied")
      stay()
    case Event(Save(_), _) =>
      sender ! AckQuorumFailed("QuorumStateUnsatisfied")
      stay()
    case Event(_: Load, _) =>
      sender ! AckQuorumFailed("QuorumStateUnsatisfied")
      stay()
    case Event(_: Iterate, _) =>
      sender ! AckQuorumFailed("QuorumStateUnsatisfied")
      stay()
    case Event(RestoreState, _) =>
      log.warning("Don't know how to restore state when quorum is unsatisfied")
      stay()
    case Event(Ready, _) =>
      sender ! false
      stay()
  }

  when(QuorumStateReadonly()){
    case Event(Get(k), data) =>
      doGet(k, sender, data)
      stay()
    case Event(RestoreState, data) =>
      val s = state(data.nodes.size)
      data.nodes.foreach(n => actorsMem.get(n, "ring_hash").fold(
        _ ! ChangeState(s), 
        _ ! ChangeState(s),
      ))
      goto(s)
    case Event(Ready, _) =>
      sender ! false
      stay()

    case Event(Put(_,_), _) =>
      sender ! AckQuorumFailed("QuorumStateReadonly")
      stay()
    case Event(Delete(_), _) =>
      sender ! AckQuorumFailed("QuorumStateReadonly")
      stay()
    case Event(Save(_), _) =>
      sender ! AckQuorumFailed("QuorumStateReadonly")
      stay()
    case Event(_: Load, _) =>
      sender ! AckQuorumFailed("QuorumStateReadonly")
      stay()
    case Event(_: Iterate, _) =>
      sender ! AckQuorumFailed("QuorumStateReadonly")
      stay()
  }

  when(QuorumStateEffective()){
    case Event(Ready, _) =>
      sender ! true
      stay()

    case Event(Get(k), data) =>
      doGet(k, sender, data)
      stay()
    case Event(Put(k,v), data) =>
      doPut(k, v, sender, data)
      stay()
    case Event(Delete(k), data) =>
      doDelete(k, sender, data)
      stay()

    case Event(Save(path), data) =>
      data.nodes.foreach(n => actorsMem.get(n, "ring_hash").fold(
        _ ! ChangeState(QuorumStateReadonly()),
        _ ! ChangeState(QuorumStateReadonly()),
      ))
      val x = system.actorOf(DumpProcessor.props(), s"dump_wrkr-${now_ms()}")
      x.forward(DumpProcessor.Save(data.buckets, local, path))
      goto(QuorumStateReadonly())
    case Event(Load(path), data) =>
      data.nodes.foreach(n => actorsMem.get(n, "ring_hash").fold(
        _ ! ChangeState(QuorumStateReadonly()),
        _ ! ChangeState(QuorumStateReadonly()),
      ))
      val x = system.actorOf(DumpProcessor.props, s"load_wrkr-${now_ms()}")
      x.forward(DumpProcessor.Load(path))
      goto(QuorumStateReadonly())
    case Event(it: Iterate, data) =>
      data.nodes.foreach(n => actorsMem.get(n, "ring_hash").fold(
        _ ! ChangeState(QuorumStateReadonly()),
        _ ! ChangeState(QuorumStateReadonly()),
      ))
      val x = system.actorOf(IterateDumpWorker.props(it), s"iter_wrkr-${now_ms()}")
      x.forward("go")
      goto(QuorumStateReadonly())

    case Event(RestoreState, _) =>
      log.info("State is already OK")
      stay()
  }

  /* COMMON FOR ALL STATES*/
  whenUnhandled {
    case Event(MemberUp(member), data) =>
      val next = joinNodeToRing(member, data)
      goto(next._1) using next._2
    case Event(MemberRemoved(member, prevState), data) =>
      val next = removeNodeFromRing(member, data)
      goto(next._1) using next._2
    case Event(Ready, data) =>
      sender ! false
      stay()
    case Event(ChangeState(s), data) =>
      state(data.nodes.size) match {
        case QuorumStateUnsatisfied() => stay()
        case _ => goto(s)
      }
    case Event(InternalPut(k, v), data) =>
      doPut(k, v, sender, data)
      stay()
  }

  def doDelete(k: Key, client: ActorRef, data: HashRngData): Unit = {
    val nodes = nodesForKey(k, data)
    val gather = system.actorOf(GatherDel.props(client, gatherTimeout, nodes, k))
    val stores = nodes.map{actorsMem.get(_, "ring_write_store")}
    stores.foreach(_.fold(
      _.tell(StoreDelete(k), gather), 
      _.tell(StoreDelete(k), gather),
    ))
  }

  def doPut(k: Key, v: Value, client: ActorRef, data: HashRngData): Unit = {
    val nodes = availableNodesFrom(nodesForKey(k, data))
    val M = nodes.size
    if (M >= W) {
      val bucket = hashing findBucket k
      val info = PutInfo(k, v, N, W, bucket, local, data.nodes)
      val gather = system.actorOf(GatherPut.props(client, gatherTimeout, actorsMem, info))
      val node = if (nodes contains local) local else nodes.head
      actorsMem.get(node, "ring_readonly_store").fold(
        _.tell(StoreGet(k), gather),
        _.tell(StoreGet(k), gather),
      )
    } else {
      client ! AckQuorumFailed("M >= W")
    }
  }

  def doGet(k: Key, client: ActorRef, data: HashRngData): Unit = {
    val nodes = availableNodesFrom(nodesForKey(k, data))
    val M = nodes.size
    if (M >= R) {
      val gather = system.actorOf(GatherGet.props(client, gatherTimeout, M, R, k))
      val stores = nodes map { actorsMem.get(_, "ring_readonly_store") }
      stores foreach (_.fold(
        _.tell(StoreGet(k), gather),
        _.tell(StoreGet(k), gather),
      ))
    } else {
      client ! AckQuorumFailed("M >= R")
    }
  }

  def availableNodesFrom(l: Set[Node]): Set[Node] = {
    val unreachableMembers = cluster.state.unreachable.map(m => m.address)
    l filterNot (node => unreachableMembers contains node)
  }

  def joinNodeToRing(member: Member, data: HashRngData): (QuorumState, HashRngData) = {
    val newvNodes: Map[VNode, Node] = (1 to vNodesNum).map(vnode => {
      hashing.hash(stob(member.address.hostPort).concat(itob(vnode))) -> member.address
    })(breakOut)
    val updvNodes = data.vNodes ++ newvNodes
    val nodes = data.nodes + member.address
    val moved = bucketsToUpdate(bucketsNum - 1, Math.min(nodes.size,N), updvNodes, data.buckets)
    data.replication map (context stop _)
    val repl = syncNodes(moved)
    val updData = HashRngData(nodes, data.buckets++moved, updvNodes, repl.some)
    log.info(s"Node ${member.address} is joining ring. Nodes in ring = ${updData.nodes.size}, state = ${state(updData.nodes.size)}")
    state(updData.nodes.size) -> updData
  }

  def removeNodeFromRing(member: Member, data: HashRngData): (QuorumState, HashRngData) = {
    log.info(s"Removing ${member} from ring")
    val unusedvNodes: Set[VNode] = (1 to vNodesNum).map(vnode => hashing.hash(stob(member.address.hostPort).concat(itob(vnode))))(breakOut)
    val updvNodes = data.vNodes.filterNot(vn => unusedvNodes.contains(vn._1))
    val nodes = data.nodes - member.address
    val moved = bucketsToUpdate(bucketsNum - 1, Math.min(nodes.size,N), updvNodes, data.buckets)
    log.info(s"Will update ${moved.size} buckets")
    data.replication map (context stop _)
    val repl = syncNodes(moved)
    val updData = HashRngData(nodes, data.buckets++moved, updvNodes, repl.some)
    state(updData.nodes.size) -> updData
  }

  def syncNodes(_buckets: SortedMap[Bucket,PreferenceList]): ActorRef = {
    val empty = SortedMap.empty[Bucket,PreferenceList]
    val buckets = _buckets.foldLeft(empty){ case (acc, (b, prefList)) =>
      if (prefList contains local) {
        prefList.filterNot(_ == local) match {
          case empty if empty.isEmpty => acc
          case prefList => acc + (b -> prefList)
        }
      } else acc
    }
    val replication = context.actorOf(ReplicationSupervisor.props(buckets), s"repl-${now_ms()}")
    replication ! "go-repl"
    replication
  }

  def state(nodes: Int): QuorumState = nodes match {
    case 0 => QuorumStateUnsatisfied()
    case n if n >= Math.max(R, W) => QuorumStateEffective()
    case _ => QuorumStateReadonly()
  }

  def bucketsToUpdate(maxBucket: Bucket, nodesNumber: Int, vNodes: SortedMap[Bucket, Node], buckets: SortedMap[Bucket, PreferenceList]): SortedMap[Bucket, PreferenceList] = {
    (0 to maxBucket).foldLeft(SortedMap.empty[Bucket, PreferenceList])((acc, b) => {
       val prefList = hashing.findNodes(b * hashing.bucketRange, vNodes, nodesNumber)
      buckets.get(b) match {
      case None => acc + (b -> prefList)
      case Some(`prefList`) => acc
      case _ => acc + (b -> prefList)
    }})
  }

  implicit val ord = Ordering.by[Node, String](n => n.hostPort)
  def nodesForKey(k: Key, data: HashRngData): PreferenceList = data.buckets.get(hashing.findBucket(k)) match {
    case None => SortedSet.empty[Node]
    case Some(nods) => nods
  }

  initialize()
}
