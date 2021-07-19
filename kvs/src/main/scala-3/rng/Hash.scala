package zd.rng

import akka.actor.*
import akka.cluster.ClusterEvent.*
import akka.cluster.{Member, Cluster}
import com.typesafe.config.Config
import zd.rng.model.{StoreDelete, StoreGet, QuorumState, ChangeState}
import zd.rng.model.QuorumState.{QuorumStateUnsatisfied, QuorumStateReadonly, QuorumStateEffective}
import scala.collection.immutable.{SortedMap, SortedSet}
import scala.concurrent.duration.*
import java.util.Arrays
import leveldbjnr.*

class Put(val k: Key, val v: Value) {
  override def equals(other: Any): Boolean = other match {
    case that: Put =>
      Arrays.equals(k, that.k) &&
      Arrays.equals(v, that.v)
    case _ => false
  }
  override def hashCode(): Int = {
    val state = Seq(k, v)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
  override def toString = s"Put(k=$k, v=$v)"
}

object Put {
  def apply(k: Key, v: Value): Put = {
    new Put(k=k, v=v)
  }
}

class Get(val k: Key) {
  override def equals(other: Any): Boolean = other match {
    case that: Get =>
      Arrays.equals(k, that.k)
    case _ => false
  }
  override def hashCode(): Int = {
    val state = Seq(k)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
  override def toString = s"Get(k=$k)"
}

object Get {
  def apply(k: Key): Get = {
    new Get(k=k)
  }
}

class Delete(val k: Key) {
  override def equals(other: Any): Boolean = other match {
    case that: Delete =>
      Arrays.equals(k, that.k)
    case _ => false
  }
  override def hashCode(): Int = {
    val state = Seq(k)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
  override def toString = s"Delete(k=$k)"
}

object Delete {
  def apply(k: Key): Delete = {
    new Delete(k=k)
  }
}

case class Save(path: String)
case class Iterate(f: (Key, Value) => Unit)
case class Load(path: String)
case class Iter(keyPrefix: Array[Byte])
case class IterRes(keys: List[String])

case object RestoreState

case object Ready

class InternalPut(val k: Key, val v: Value) {
  override def equals(other: Any): Boolean = other match {
    case that: InternalPut =>
      Arrays.equals(k, that.k) &&
      Arrays.equals(v, that.v)
    case _ => false
  }
  override def hashCode(): Int = {
    val state = Seq(k, v)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
  override def toString = s"InternalPut(k=$k, v=$v)"
}

object InternalPut {
  def apply(k: Key, v: Value): InternalPut = {
    new InternalPut(k=k, v=v)
  }
}

case class HashRngData(
  nodes: Set[Node],
  buckets: SortedMap[Bucket, PreferenceList],
  vNodes: SortedMap[Bucket, Node],
  replication: Option[ActorRef],
)

object Hash {
  def props(leveldb: LevelDb): Props = Props(new Hash(leveldb))
}

// TODO available/not avaiable nodes
class Hash(leveldb: LevelDb) extends FSM[QuorumState, HashRngData] with ActorLogging {
  import context.system

  val config: Config = system.settings.config.getConfig("ring").nn

  val quorum = config.getIntList("quorum").nn
  val N: Int = quorum.get(0).nn
  val W: Int = quorum.get(1).nn
  val R: Int = quorum.get(2).nn
  val gatherTimeout = Duration.fromNanos(config.getDuration("gather-timeout").nn.toNanos)
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

  startWith(QuorumStateUnsatisfied, HashRngData(Set.empty[Node], SortedMap.empty[Bucket, PreferenceList], SortedMap.empty[Bucket, Node], replication=None))

  override def preStart() = {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents, classOf[MemberUp], classOf[MemberRemoved])
  }
  
  override def postStop(): Unit = cluster.unsubscribe(self)

  when(QuorumStateUnsatisfied){
    case Event(_: Get, _) =>
      sender ! AckQuorumFailed("QuorumStateUnsatisfied")
      stay()
    case Event(_: Put, _) =>
      sender ! AckQuorumFailed("QuorumStateUnsatisfied")
      stay()
    case Event(_: Delete, _) =>
      sender ! AckQuorumFailed("QuorumStateUnsatisfied")
      stay()
    case Event(Save(_), _) =>
      sender ! AckQuorumFailed("QuorumStateUnsatisfied")
      stay()
    case Event(Iterate(_), _) =>
      sender ! AckQuorumFailed("QuorumStateUnsatisfied")
      stay()
    case Event(Load(_), _) =>
      sender ! AckQuorumFailed("QuorumStateUnsatisfied")
      stay()
    case Event(Iter(_), _) =>
      sender ! AckQuorumFailed("QuorumStateUnsatisfied")
      stay()
    case Event(RestoreState, _) =>
      log.warning("Don't know how to restore state when quorum is unsatisfied")
      stay()
    case Event(Ready, _) =>
      sender ! false
      stay()
  }

  when(QuorumStateReadonly){
    case Event(x: Get, data) =>
      doGet(x.k, sender, data)
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

    case Event(_: Put, _) =>
      sender ! AckQuorumFailed("QuorumStateReadonly")
      stay()
    case Event(_: Delete, _) =>
      sender ! AckQuorumFailed("QuorumStateReadonly")
      stay()
    case Event(Save(_), _) =>
      sender ! AckQuorumFailed("QuorumStateReadonly")
      stay()
    case Event(Iterate(_), _) =>
      sender ! AckQuorumFailed("QuorumStateReadonly")
      stay()
    case Event(Load(_), _) =>
      sender ! AckQuorumFailed("QuorumStateReadonly")
      stay()
    case Event(Iter(_), _) =>
      sender ! AckQuorumFailed("QuorumStateReadonly")
      stay()
  }

  when(QuorumStateEffective){
    case Event(Ready, _) =>
      sender ! true
      stay()

    case Event(x: Get, data) =>
      doGet(x.k, sender, data)
      stay()
    case Event(x: Put, data) =>
      doPut(x.k, x.v, sender, data)
      stay()
    case Event(x: Delete, data) =>
      doDelete(x.k, sender, data)
      stay()

    case Event(Save(path), data) =>
      data.nodes.foreach(n => actorsMem.get(n, "ring_hash").fold(
        _ ! ChangeState(QuorumStateReadonly),
        _ ! ChangeState(QuorumStateReadonly),
      ))
      val x = system.actorOf(DumpProcessor.props(), s"dump_wrkr-${now_ms()}")
      x.forward(DumpProcessor.Save(data.buckets, path))
      goto(QuorumStateReadonly)
    case Event(Iterate(f), data) =>
      data.nodes.foreach(n => actorsMem.get(n, "ring_hash").fold(
        _ ! ChangeState(QuorumStateReadonly),
        _ ! ChangeState(QuorumStateReadonly),
      ))
      val x = system.actorOf(DumpProcessor.props(), s"dump_wrkr-${now_ms()}")
      x.forward(DumpProcessor.Iterate(data.buckets, f))
      goto(QuorumStateReadonly)
    case Event(Load(path), data) =>
      data.nodes.foreach(n => actorsMem.get(n, "ring_hash").fold(
        _ ! ChangeState(QuorumStateReadonly),
        _ ! ChangeState(QuorumStateReadonly),
      ))
      val x = system.actorOf(DumpProcessor.props(), s"load_wrkr-${now_ms()}")
      x.forward(DumpProcessor.Load(path))
      goto(QuorumStateReadonly)
    case Event(Iter(keyPrefix), data) =>
      val it = leveldb.iter()
      it.seek_to_first()
      var keys = collection.mutable.ListBuffer.empty[String]
      while (it.valid()) {
        val key = it.key()
        val idx = key.indexOfSlice(keyPrefix)
        if (idx >= 0) {
          val slice = key.slice(idx, key.length)
          keys += new String(slice, "utf8")
        }
        it.next()
      }
      it.close()
      sender ! IterRes(keys.toList)
      stay()
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
        case QuorumStateUnsatisfied => stay()
        case _ => goto(s)
      }
    case Event(x: InternalPut, data) =>
      doPut(x.k, x.v, sender, data)
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
      val bucket = hashing `findBucket` k
      val info = PutInfo(k, v, N, W, bucket, local, data.nodes)
      val gather = system.actorOf(GatherPut.props(client, gatherTimeout, info))
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
    val newvNodes: Map[VNode, Node] = (1 to vNodesNum).view.map(vnode => {
      hashing.hash(stob(member.address.hostPort).++(itob(vnode))) -> member.address
    }).to(Map)
    val updvNodes = data.vNodes ++ newvNodes
    val nodes = data.nodes + member.address
    val moved = bucketsToUpdate(bucketsNum - 1, Math.min(nodes.size,N), updvNodes, data.buckets)
    data.replication map (context stop _)
    val repl = syncNodes(moved)
    val updData = HashRngData(nodes, data.buckets++moved, updvNodes, Some(repl))
    log.info(s"Node ${member.address} is joining ring. Nodes in ring = ${updData.nodes.size}, state = ${state(updData.nodes.size)}")
    state(updData.nodes.size) -> updData
  }

  def removeNodeFromRing(member: Member, data: HashRngData): (QuorumState, HashRngData) = {
    log.info(s"Removing ${member} from ring")
    val unusedvNodes: Set[VNode] = (1 to vNodesNum).view.map(vnode => hashing.hash(stob(member.address.hostPort).++(itob(vnode)))).to(Set)
    val updvNodes = data.vNodes.filterNot(vn => unusedvNodes.contains(vn._1))
    val nodes = data.nodes - member.address
    val moved = bucketsToUpdate(bucketsNum - 1, Math.min(nodes.size,N), updvNodes, data.buckets)
    log.info(s"Will update ${moved.size} buckets")
    data.replication map (context stop _)
    val repl = syncNodes(moved)
    val updData = HashRngData(nodes, data.buckets++moved, updvNodes, Some(repl))
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
    case 0 => QuorumStateUnsatisfied
    case n if n >= Math.max(R, W) => QuorumStateEffective
    case _ => QuorumStateReadonly
  }

  def bucketsToUpdate(
    maxBucket: Bucket
  , nodesNumber: Int
  , vNodes: SortedMap[Bucket, Node]
  , buckets: SortedMap[Bucket, PreferenceList]
  ): SortedMap[Bucket, PreferenceList] =
    (0 to maxBucket).foldLeft(SortedMap.empty[Bucket, PreferenceList])(
      (acc, b) => {
        val prefList = hashing.findNodes(b * hashing.bucketRange, vNodes, nodesNumber)
        buckets.get(b) match {
          case None => acc + (b -> prefList)
          case Some(`prefList`) => acc
          case _ => acc + (b -> prefList)
        }
      }
    )

  implicit val ord: Ordering[Node] = Ordering.by[Node, String](n => n.hostPort)
  def nodesForKey(k: Key, data: HashRngData): PreferenceList = data.buckets.get(hashing.findBucket(k)) match {
    case None => SortedSet.empty[Node]
    case Some(nods) => nods
  }

  initialize()
}
