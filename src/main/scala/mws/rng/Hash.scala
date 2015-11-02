package mws.rng

import akka.actor._
import akka.cluster.ClusterEvent._
import akka.cluster.{Cluster, ClusterEvent, MemberStatus}
import akka.pattern.ask
import akka.util.Timeout
import scala.annotation.tailrec
import scala.collection.SortedMap
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.collection.JavaConversions._

/**
 * Partitioning. Not considering physical placement
 *
 * Consistent hashing
 * - 32 hash space
 * - Virtual nodes
 * - Buckets
 * # rehash when membership changes
 * # find node associated with key for get/put
 */

sealed class HashMessage
case class Put(k: Key, v: Value) extends HashMessage
case class Get(k: Key) extends HashMessage
case class Delete(k: Key) extends HashMessage
case object Ready

class Hash(localWStore: ActorRef, localRStore: ActorRef ) extends Actor with ActorLogging {
  import context.system

  val config = system.settings.config.getConfig("ring")
  log.info(s"Ring configuration: ")
  for (c <- config.entrySet()){ log.info(s"${c.getKey} = ${c.getValue.render()}")}

  implicit val timeout = Timeout(5.second)

  val quorum = config.getIntList("quorum")  //N,W,R
  val N: Int = quorum.get(0)
  val W: Int = quorum.get(1)
  val R: Int = quorum.get(2)
  val gatherTimeout = config.getInt("gather-timeout")
  val vNodesNum = config.getInt("virtual-nodes")
  val bucketsNum = config.getInt("buckets")
  val cluster = Cluster(system)
  val local:Address = cluster.selfAddress
  val hashing = HashingExtension(system)
  val actorsMem = new SelectionMemorize(system)

  @volatile
  private var initialized = false
  @volatile
  private var state:CurrentClusterState = CurrentClusterState()
  @volatile
  private var vNodes:SortedMap[Bucket,Address] = SortedMap.empty[Bucket, Address]
  @volatile
  private var buckets:SortedMap[Bucket, PreferenceList] = SortedMap.empty // {bucket-to-replicas }

  override def preStart() = {
    cluster.subscribe(self, ClusterEvent.initialStateAsEvents, classOf[ClusterDomainEvent], classOf[CurrentClusterState])
    cluster.sendCurrentClusterState(self)

    (1 to vNodesNum).foreach(vnode => {
      val hashedKey = hashing.hash(Left(local, vnode))
      vNodes += hashedKey -> local})
    updateStrategy(bucketsToUpdate)
    initialized = true
    log.info("Ring: complete initial partitioning")
  }

  override def postStop(): Unit = cluster.unsubscribe(self)

  override def receive: Receive = receiveCl orElse receiveApi

  def receiveApi: Receive = {
    case Put(k, v) => doPut(k, v, sender())
    case Get(k) => doGet(k, sender())
    case Delete(k) => doDelete(k, sender())
  }

  private[mws] def doPut(k: Key, v: Value, client: ActorRef) = {
    val bucket = hashing.findBucket(Left(k))
    val nodsFrom = availableNodesFrom(findNodes(Left(k)))

    if (nodsFrom.size >= W) {
      val info: PutInfo = PutInfo(k, v, N, W, bucket, local, nodsFrom)
      val gather = system.actorOf(GatherPutFSM.props(client, gatherTimeout, actorsMem, info))
      localRStore ! LocalStoreGet(k, gather)
    } else {
      client ! AckQuorumFailed
    }
  }

  private[mws] def doGet(key: Key, client: ActorRef) = {
    val fromNodes = availableNodesFrom(findNodes(Left(key)))
    val refs = fromNodes map { actorsMem.get(_, "ring_readonly_store") }
    if(refs.nonEmpty){
      val gather = system.actorOf(Props(classOf[GatherGetFsm], client, N, R, gatherTimeout, actorsMem))
      refs map (store => store.fold(
        _.tell(StoreGet(key), gather),
        _.tell(StoreGet(key), gather)))
    }else {
      client ! None
    }
  }

  private[mws] def doDelete(k: Key, client: ActorRef) = {
    import context.dispatcher
    val nodes = findNodes(Left(k))
      val deleteF = Future.traverse(availableNodesFrom(nodes))(n =>
        (system.actorSelection(RootActorPath(n) / "user" / "ring_store") ? StoreDelete(k)).mapTo[String])
      deleteF.map(statuses => system.actorSelection("/user/ring_gatherer") ! GatherDel(statuses, client))
  }

  def receiveCl: Receive = {
    case e:ClusterDomainEvent => cluster.sendCurrentClusterState(self)
      e match {
      case MemberUp(member) =>
        log.info(s"=>[ring_hash] Node ${member.address} is joining ring")
        (1 to vNodesNum).foreach(vnode => {
          val hashedKey = hashing.hash(Left(member.address, vnode))
          vNodes += hashedKey -> member.address})
        updateStrategy(bucketsToUpdate)
      case MemberRemoved(member, prevState) =>
        log.info(s"[ring_hash]Removing $member from ring")
        val hashes = (1 to vNodesNum).map(v => hashing.hash(Left((member.address, v))))
        vNodes = vNodes.filterNot(vn =>  hashes.contains(vn._1))
        updateStrategy(bucketsToUpdate)
      case _ =>
      }

    case Ready => sender() ! initialized
    case s: CurrentClusterState => state = s
  }

  private def availableNodesFrom(l: List[Node]) = {
    val unreachableMembers = state.unreachable.map(m => m.address)
    l filterNot (node => unreachableMembers contains node)
  }

  def bucketsToUpdate: List[SynchReplica] = {
    val maxSearch = if (nodesInRing == 1) 1 else vNodes.size // don't search other nodes to fill the bucket when 1 node
    log.info(s"[hash] nodes in ring = $nodesInRing")
    bucketsToUpdate(bucketsNum - 1, maxSearch, List.empty)
  }

  @tailrec
  private def bucketsToUpdate(bucket: Bucket, max: Int, hasBeenMoved: List[SynchReplica]): List[SynchReplica] = bucket match {
    case -1 => hasBeenMoved
    case bucket: Int =>
      val newNodes = findBucketNodes(bucket * hashing.bucketRange, max, nodesInRing, Nil)
      buckets.get(bucket) match {
        case Some(`newNodes`) =>
          bucketsToUpdate(bucket - 1, max, hasBeenMoved)
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
          bucketsToUpdate(bucket - 1, max, replacedUpd)
      }
  }

  @tailrec
  private def findBucketNodes(hashedKey: Int, maxSearch: Int, nodesAvailable: Int, nodes: List[Node]): List[Node] = maxSearch match {
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

  def nodesInRing: Int = {
    state.members.count(m => m.status == MemberStatus.Up) + 1 // state doesn't calculates self node.
  }

  def findNodes(keyOrBucket: Either[Key, Bucket]): List[Node] = {
    val bucket: Bucket = keyOrBucket match {
      case Left(k: Key) => hashing.findBucket(Left(k))
      case Right(b: Bucket) => b
    }

    buckets.get(bucket) match {
      case Some(nods) => nods
      case _ => Nil
    }
  }

  @tailrec
  private def updateStrategy(buckets: List[SynchReplica]): Unit = buckets match {
      case Nil => // done synch
      case (bucket, newReplica, oldReplica) :: tail =>
        (newReplica, oldReplica) match {
          case (`newReplica`, None) =>
            updateBucket(bucket, findNodes(Right(bucket)).filterNot(_ == local))
          case (None, `oldReplica`) => // we don't responsible for this buckets
          case _ => //nop
        }
        updateStrategy(tail)
    }

  private def updateBucket(bucket: Bucket, nodes: List[Node]): Unit = {
    import context.dispatcher
    val storesOnNodes = nodes.map { actorsMem.get(_, "ring_store") }
    val bucketsDataF = Future.traverse(storesOnNodes)(n => n.fold(
      _ ? BucketGet(bucket),
      _ ? BucketGet(bucket))).mapTo[List[List[Data]]]

    bucketsDataF map {
      case l if l.isEmpty || l.forall(_ == Nil) =>
      case l => localWStore ! BucketPut(mergeData(l.flatten, Nil))
    }
  }

  @tailrec
  private def mergeData(l: List[Data], merged: List[Data]): List[Data] = l match {
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
