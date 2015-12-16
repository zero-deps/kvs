package mws.rng

import akka.actor._
import akka.cluster.ClusterEvent._
import akka.cluster.{Member, Cluster, ClusterEvent}
import akka.pattern.ask
import akka.util.Timeout
import scala.annotation.tailrec
import scala.collection.SortedMap
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.collection.JavaConversions._

sealed class RingMessage
//kvs
case class Put(k: Key, v: Value) extends RingMessage
case class Get(k: Key) extends RingMessage
case class Delete(k: Key) extends RingMessage
//feed
case class Add(feed: String, v: Value) extends RingMessage
case class Traverse(fid: String, start: Option[Int], end: Option[Int]) extends RingMessage
case class Remove(nb: String, v: Value) extends RingMessage
case class RegisterFeed(feed: String) extends RingMessage

//utilities
case object Ready

class Hash(localWStore: ActorRef, localRStore: ActorRef) extends Actor with ActorLogging {

  import context.system

  val config = system.settings.config.getConfig("ring")
  log.info(s"Ring configuration: ")
  for (c <- config.entrySet()) {
    log.info(s"${c.getKey} = ${c.getValue.render()}")
  }

  implicit val timeout = Timeout(5.second)

  val quorum = config.getIntList("quorum")
  //N,W,R
  val N: Int = quorum.get(0)
  val W: Int = quorum.get(1)
  val R: Int = quorum.get(2)
  val gatherTimeout = config.getInt("gather-timeout")
  val vNodesNum = config.getInt("virtual-nodes")
  val bucketsNum = config.getInt("buckets")
  val cluster = Cluster(system)
  val local: Address = cluster.selfAddress
  val hashing = HashingExtension(system)
  val actorsMem = new SelectionMemorize(system)

  var state: CurrentClusterState = CurrentClusterState()
  var vNodes: SortedMap[Bucket, Address] = SortedMap.empty[Bucket, Address]
  var buckets: SortedMap[Bucket, PreferenceList] = SortedMap.empty
  var feeds: SortedMap[FeedId, PreferenceList] = SortedMap.empty
  var processedNodes = Set.empty[Member]
  val feedSuperviser = system.actorOf(Props(classOf[FeedsSupervisor], actorsMem))

  override def preStart() = {
    cluster.subscribe(self, ClusterEvent.initialStateAsEvents, classOf[ClusterDomainEvent], classOf[CurrentClusterState])
    cluster.sendCurrentClusterState(self)
  }

  override def postStop(): Unit = cluster.unsubscribe(self)

  override def receive: Receive = ready

  def ready = receiveCl orElse receiveApi
  def prepearing = notReady orElse receiveApi

  def notReady: Receive =  {
    case Ready => sender ! false
    case msg:RingMessage => //ignore all messages
  }

  def receiveApi: Receive = {
    case Put(k, v) => doPut(k, v, sender())
    case Get(k) => doGet(k, sender())
    case Delete(k) => doDelete(k, sender())
    case Add(fid, v) => feedSuperviser.tell(AddToFeed(fid, v, nodesForKey(fid)), sender())
    case Traverse(fid, start, end) => feedSuperviser.tell(TraverseFeed(fid, nodesForKey(fid), start, end), sender())

    case RegisterFeed(feed) => {
      feeds = feeds + (feed -> nodesForKey(feed))
    }
  }

  def doPut(k: Key, v: Value, client: ActorRef):Unit = {
    val bucket = hashing.findBucket(Left(k))
    val nodsFrom = availableNodesFrom(nodesForKey(k))

    if (nodsFrom.size >= W) {
      val info: PutInfo = PutInfo(k, v, N, W, bucket, local, nodsFrom)
      val gather = system.actorOf(GatherPutFSM.props(client, gatherTimeout, actorsMem, info))
      localRStore ! LocalStoreGet(k, gather)
    } else {
      client ! AckQuorumFailed
    }
  }

  def doGet(key: Key, client: ActorRef) : Unit = {
    val fromNodes = availableNodesFrom(nodesForKey(key))
    val refs = fromNodes map {
      actorsMem.get(_, "ring_readonly_store")
    }
    if (refs.nonEmpty) {
      val gather = system.actorOf(Props(classOf[GatherGetFsm], client, fromNodes.size, R, gatherTimeout, actorsMem))
      refs foreach (store => store.fold(
        _.tell(StoreGet(key), gather),
        _.tell(StoreGet(key), gather)))
    } else {
      client ! None
    }
  }

  def doDelete(k: Key, client: ActorRef) : Unit = {
    import context.dispatcher
    val nodes = nodesForKey(k)
    val deleteF = Future.traverse(availableNodesFrom(nodes))(n =>
      (system.actorSelection(RootActorPath(n) / "user" / "ring_write_store") ? StoreDelete(k)).mapTo[String])
    deleteF.map(statuses => system.actorSelection("/user/ring_gatherer") ! GatherDel(statuses, client))
  }

  def receiveCl: Receive = {
    case e: ClusterDomainEvent => cluster.sendCurrentClusterState(self)
      e match {
        case MemberUp(member) =>
          processedNodes = processedNodes + member
          (1 to vNodesNum).foreach(vnode => {
            val hashedKey = hashing.hash(member.address.hostPort + vnode)
            vNodes += hashedKey -> member.address
          })
          synchNodes(bucketsToUpdate(member.address))
          // TODO synch named bucket
          log.info(s"=>[ring_hash] Node ${member.address} is joining ring")
        case UnreachableMember(member) =>
          // TODO synch named bucket
          processedNodes = processedNodes - member
          log.info(s"[ring_hash] $member become unreachable among cluster and ring")
          val hashes = (1 to vNodesNum).map(v => hashing.hash(member.address.hostPort + v))
          vNodes = vNodes.filterNot(vn => hashes.contains(vn._1))
          synchNodes(bucketsToUpdate(member.address))
        case MemberRemoved(member, prevState) =>
          // TODO synch named bucket
          processedNodes = processedNodes - member
          log.info(s"[ring_hash]Removing $member from ring")
          val hashes = (1 to vNodesNum).map(v => hashing.hash(member.address.hostPort + v))
          vNodes = vNodes.filterNot(vn => hashes.contains(vn._1))
          synchNodes(bucketsToUpdate(member.address))
        case _ =>
      }

    case Ready => sender() ! (state.members.size == processedNodes.size)
    case s: CurrentClusterState => state = s
  }

  def availableNodesFrom(l: List[Node]): List[Node] = {
    val unreachableMembers = state.unreachable.map(m => m.address)
    l filterNot (node => unreachableMembers contains node)
  }

  def bucketsToUpdate(newjoiner: Node): List[SynchReplica] = {
    val maxSearch = if (nodesInRing() == 1) 1 else vNodes.size // don't search other nodes to fill the bucket when 1 node
    log.info(s"[hash] nodes in ring = ${nodesInRing()}")
    bucketsToUpdate(bucketsNum - 1, maxSearch, nodesInRing(), List.empty)
  }

  @tailrec
  final def bucketsToUpdate(bucket: Bucket, max: Int, nodesCount: Int, hasBeenMoved: List[SynchReplica]): List[SynchReplica] = bucket match {
    case -1 => hasBeenMoved
    case bucket: Int =>
      val newNodes = findBucketNodes(bucket * hashing.bucketRange, max, nodesCount, Nil)
      buckets.get(bucket) match {
        case Some(`newNodes`) =>
          bucketsToUpdate(bucket - 1, max, nodesCount, hasBeenMoved)
        case outdatedNodes =>
          buckets += bucket -> newNodes
          val isResponsibleNow = newNodes.indexOf(cluster.selfAddress) match {
            case -1 => None
            case i => Some(i)
          }
          val wasResponsible = outdatedNodes match {
            case Some(oldNodes) =>
              oldNodes.indexOf(cluster.selfAddress) match {
                case -1 => None
                case i => Some(i)
              }
            case None => None
          }

          val replacedUpd = (wasResponsible, isResponsibleNow) match {
            case (_, `wasResponsible`) => hasBeenMoved // my responsibility not changed.
            case _ => (bucket, isResponsibleNow, wasResponsible) :: hasBeenMoved
          }
          bucketsToUpdate(bucket - 1, max, nodesCount, replacedUpd)
      }
  }

  @tailrec
  final def findBucketNodes(hashedKey: Int, maxSearch: Int, nodesAvailable: Int, nodes: List[Node]): List[Node] = maxSearch match {
    case 0 => nodes.reverse
    case _ =>
      val it = vNodes.keysIteratorFrom(hashedKey)
      val hashedNode = if (it.hasNext) it.next() else vNodes.firstKey
      val node = vNodes.get(hashedNode).get
      val prefList = if (nodes.contains(node)) nodes else node :: nodes

      prefList.length match {
        case `N` => prefList.reverse
        case `nodesAvailable` => prefList.reverse
        case _ => findBucketNodes(hashedNode + 1, nodesAvailable, maxSearch - 1, prefList)
      }
  }

  def nodesInRing(): Int = processedNodes.size

  def nodesForKey(k: Key): List[Node] = buckets.get(hashing.findBucket(Left(k))) match {
    case Some(nods) => nods
    case _ => Nil
  }

  @tailrec
  final def synchNodes(buckets: List[SynchReplica]): Unit = buckets match {
    case Nil => // done synch
    case (bucket, newReplica, oldReplica) :: tail =>
      (newReplica, oldReplica) match {
        case (`newReplica`, None) =>
          updateBucket(bucket, this.buckets(bucket).filterNot(_ == local))
        case (None, `oldReplica`) => // we don't responsible for this buckets
        case _ => //nop
      }
      synchNodes(tail)
  }

  def updateBucket(bucket: Bucket, nodes: List[Node]): Unit = {
    import context.dispatcher
    val storesOnNodes = nodes.map {
      actorsMem.get(_, "ring_write_store")
    }
    val bucketsDataF = Future.traverse(storesOnNodes)(n => n.fold(
      _ ? BucketGet(bucket),
      _ ? BucketGet(bucket))).mapTo[List[List[Data]]]

    bucketsDataF map {
      case l if l.isEmpty || l.forall(_ == Nil) =>
      case l => localWStore ! BucketPut(mergeData(l.flatten, Nil))
    }
  }

  @tailrec
  final def mergeData(l: List[Data], merged: List[Data]): List[Data] = l match {
    case h :: t =>
      merged.find(_.key == h.key) match {
        case Some(d) if h.vc == d.vc && h.lastModified > d.lastModified =>
          mergeData(t, h :: merged.filterNot(_.key == h.key))
        case Some(d) if h.vc > d.vc =>
          mergeData(t, h :: merged.filterNot(_.key == h.key))
        case None => mergeData(t, h :: merged)
        case _ => mergeData(t, merged)
      }
    case Nil => merged
  }
}
