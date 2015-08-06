package mws.rng

import akka.actor._
import akka.cluster.ClusterEvent._
import akka.cluster.{Cluster, ClusterEvent, MemberStatus, VectorClock}
import akka.pattern.ask
import akka.util.Timeout

import scala.annotation.tailrec
import scala.collection.SortedMap
import scala.concurrent.duration._
import scala.concurrent.Future

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

class Hash(localStore: ActorRef) extends Actor with ActorLogging {
  import context.system

  val config = system.settings.config.getConfig("ring")
  implicit val timeout = Timeout(3.second)

  val quorum = config.getIntList("quorum")  //N,R,W
  log.info(s"q = $quorum")
  val N: Int = quorum.get(0)
  val vNodesNum = config.getInt("virtual-nodes")
  log.info(s"vNodesNum = $vNodesNum")
  val bucketsNum = config.getInt("buckets")
  log.info(s"bucketsNum = $bucketsNum")
  val rngRoleName = config.getString("ring-node-name")
  log.info(s"nodeRoleName = $rngRoleName")
  val cluster = Cluster(system)
  val local:Address = cluster.selfAddress
  val hashing = HashingExtension(system)

  @volatile
  private var state:CurrentClusterState = CurrentClusterState()
  @volatile
  private var vNodes:SortedMap[Int,Address] = SortedMap.empty[Int, Address]
  @volatile
  private var buckets:SortedMap[Int, PreferenceList] = SortedMap.empty // {bucket-to-replicas }

  override def preStart() = {
    cluster.subscribe(self, ClusterEvent.initialStateAsEvents, classOf[ClusterDomainEvent], classOf[CurrentClusterState])
    cluster.sendCurrentClusterState(self)
  }

  override def postStop(): Unit = cluster.unsubscribe(self)

  override def receive: Receive = receiveCl orElse receiveApi

  def receiveApi: Receive = {
    case Put(k, v) =>doPut(k, v, sender())
    case Get(k) => doGet(k, sender())
    case Delete(k) => doDelete(k, sender())
  }

  private[mws] def doPut(k: Key, v: Value, client: ActorRef) = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val bucket = hashing.findBucket(Left(k))
    val nodes = findNodes(Left(k))
    localStore ? StoreGet(k) onSuccess {
      case data : Option[List[Data]] => 
        
      val vc: VectorClock = data match {
        case Some(d) if d.size == 1 => d.head.vc
        case Some(d) if d.size > 1 => (d map (_.vc)).foldLeft(new VectorClock)((sum, i) => sum.merge(i))
        case None => new VectorClock
      }
      val updatedData = Data(k, bucket, System.currentTimeMillis(), vc.:+(local.toString), v)
      mapInPut(nodes, updatedData, client)
    }

  }

  def flat(tuples: List[(Option[List[Data]], Node)], res: List[(Option[Data], Node)]): List[(Option[Data], Node)] = {
    tuples match {
      case h :: t => h._1 match {
        case Some(l) =>
          val list: List[(Some[Data], Node)] = l map (d => (Some(d), h._2))
          flat(t, list ++ res)
        case None => flat(t, (None, h._2) :: res)
      }
      case Nil => res
    }
  }

  private[mws] def doGet(key: Key, client: ActorRef) = {
    val nodes = findNodes(Left(key))
    import context.dispatcher
      val resultsF = Future.traverse(availableNodesFrom(nodes))(node =>
        (system.actorSelection(RootActorPath(node) / "user" / "ring_store") ? StoreGet(key))
          .mapTo[Option[List[Data]]].map(data => (data, node)))
        resultsF.map(dataList => system.actorSelection("/user/ring_gatherer") ! GatherGet(flat(dataList, Nil), client))
  }

  private[mws] def doDelete(k: Key, client: ActorRef) = {
    import context.dispatcher
    val nodes = findNodes(Left(k))
      val deleteF = Future.traverse(availableNodesFrom(nodes))(n =>
        (system.actorSelection(RootActorPath(n) / "user" / "ring_store") ? StoreDelete(k)).mapTo[String])
      deleteF.map(statuses => system.actorSelection("/user/ring_gatherer") ! GatherDel(statuses, client))

  }

  def receiveCl: Receive = {
    case e:ClusterDomainEvent => cluster.sendCurrentClusterState(self) // TODO bind to another event to avoid unnecessary spam.
      e match {
      case MemberUp(member) if member.hasRole(rngRoleName) =>
        log.info(s"=>[ring_hash] Node ${member.address} is joining ring")
        (1 to vNodesNum).foreach(vnode => {
          val hashedKey = hashing.hash(Left(member.address, vnode))
          vNodes += hashedKey -> member.address})
        syncBuckets(bucketsToUpdate)
      case MemberRemoved(member, prevState) if member.hasRole(rngRoleName) =>
        log.info(s"[ring_hash]Removing $member from ring")
        val hashes = (1 to vNodesNum).map(v => hashing.hash(Left((member.address, v))))
        vNodes = vNodes.filterNot(vn =>  hashes.contains(vn._1))
        syncBuckets(bucketsToUpdate)
      case _ =>
      }
    case s: CurrentClusterState => state = s
  }

  private def availableNodesFrom(l: List[Node]) = {
    val unreachableMembers = state.unreachable.map(m => m.address)
    l filterNot (node => unreachableMembers contains node)
  }

  private[mws] def mapInPut(nodes: List[Node], updatedData: Data, client: ActorRef) = {
    import context.dispatcher
    val putFutures = Future.traverse(availableNodesFrom(nodes))(n =>
      (system.actorSelection(RootActorPath(n) / "user" / "ring_store") ? StorePut(updatedData)).mapTo[String])
    putFutures map (statuses => system.actorSelection("/user/ring_gatherer") ! GatherPut(statuses, client))
  }

  def bucketsToUpdate: List[SynchReplica] = {
    val maxSearch = if (nodesInRing == 1) 1 else vNodes.size // don't search other nodes to fill the bucket when 1 node
    bucketsToUpdate(bucketsNum - 1, maxSearch, List.empty)
  }

  @tailrec
  private def bucketsToUpdate(bucket: Bucket, max: Int, replaced: List[SynchReplica]): List[SynchReplica] = bucket match {
    case -1 => replaced
    case bucket: Int =>
      val newNodes = searchBucketNodes(bucket * hashing.bucketRange, max, nodesInRing, Nil)
      buckets.get(bucket) match {
        case Some(`newNodes`) =>
          bucketsToUpdate(bucket - 1, max, replaced)
        case outdatedNodes =>
          buckets += bucket -> newNodes
          val newReplica = newNodes.indexOf(cluster.selfAddress) match {
            case -1 => None
            case i: Int => Some(i)
          }
          val oldReplica: Option[Int] = outdatedNodes match { // TODO do we need?
            case Some(oldNodes) =>
              oldNodes.indexOf(cluster.selfAddress) match {
                case -1 => None
                case i: Int => Some(i)
              }
            case None => None
          }

          val replacedUpd = (oldReplica, newReplica) match {
            case (_, `oldReplica`) => replaced
            case _ => (bucket, newReplica, oldReplica) :: replaced
          }
          bucketsToUpdate(bucket - 1, max, replacedUpd)
      }
  }

  @tailrec
  private def searchBucketNodes(hashedKey: Int, maxSearch: Int, nodesAvailable: Int, nodes: List[Node]): List[Node] = {
    maxSearch match {
      case 0 => nodes.reverse
      case _ =>
        val it = vNodes.keysIteratorFrom(hashedKey)
        val hashedNode = if (it.hasNext) it.next() else vNodes.firstKey
        val node = vNodes.get(hashedNode).get
        val prefList = if (nodes.contains(node)) nodes else node :: nodes

        prefList.length match {
          case `N` => prefList.reverse
          case `nodesAvailable` => prefList.reverse
          case _ => searchBucketNodes(hashedNode + 1, nodesAvailable, maxSearch - 1, prefList)
        }
    }
  }

  def nodesInRing: Int = {
    state.members.count(m => m.status == MemberStatus.Up && m.hasRole(rngRoleName)) + 1 // state doesn't calculates current node.
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
  private def syncBuckets(buckets: List[SynchReplica]): Unit =
    buckets match {
      case Nil => // done synch
      case (bucket, newReplica, oldReplica) :: tail =>
        (newReplica, oldReplica) match {
          case (`newReplica`, None) =>
            updateBuckets(bucket, findNodes(Right(bucket)).filterNot(_ == cluster.selfAddress))
          case (None, `oldReplica`) => localStore ! BucketDelete(bucket)
          case _ => //nop
        }
        syncBuckets(tail)
    }

  private def updateBuckets(bucket: Bucket, nodes: List[Node]): Unit = {
    import context.dispatcher
    val bucketsDataF = Future.traverse(nodes)(n => system.actorSelection(RootActorPath(n) / "user" / "ring_store").?(BucketGet(bucket))).mapTo[List[List[Data]]]

    bucketsDataF map {
      case l if l.isEmpty || l.forall(_ == Nil) =>
      case l => localStore ! BucketPut(mergeData(l.flatten, Nil))
    }
  }

  def mergeData(l: List[Data], n: List[Data]): List[Data] = l match {
    case h :: t =>
      n.find(_.key == h.key) match {
        case Some(d) if h.vc == d.vc && h.lastModified > d.lastModified =>
          mergeData(t, h :: n.filterNot(_.key == h.key))
        case Some(d) if h.vc > d.vc =>
          mergeData(t, h :: n.filterNot(_.key == h.key))
        case None => mergeData(t, h :: n)
        case _ => mergeData(t, n)
      }
    case Nil => n
  }
  
}
