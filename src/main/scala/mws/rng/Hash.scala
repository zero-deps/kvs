package mws.rng

import akka.actor._
import akka.cluster.ClusterEvent._
import akka.cluster.{Cluster, ClusterEvent, MemberStatus, VectorClock}
import akka.pattern.ask
import akka.util.Timeout

import scala.annotation.tailrec
import scala.collection.SortedMap
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

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
case class Put(k: Key, v: Value)

case class Get(k: Key)

case class Delete(k: Key)

class Hash extends Actor with ActorLogging {
  import context.system

  val config = system.settings.config.getConfig("ring")
  implicit val timeout = Timeout(1.second)

  val quorum = config.getIntList("quorum")  //N,R,W
  log.info(s"q = $quorum")
  val N: Int = quorum.get(0)
  val R: Int = quorum.get(1)
  val W: Int = quorum.get(2)
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
  private var buckets:SortedMap[Int, RingBucket] = SortedMap.empty // {bucket-to-replicas }

  override def preStart() = {
    cluster.subscribe(self, ClusterEvent.initialStateAsEvents, classOf[ClusterDomainEvent], classOf[CurrentClusterState])
    cluster.sendCurrentClusterState(self)
  }

  override def postStop(): Unit = cluster.unsubscribe(self)

  override def receive: Receive = receiveCl orElse receiveApi

  def receiveApi: Receive = {
    case Put(k, v) => sender() ! doPut(k, v)
    case Get(k) => doGet(k, sender())
    case Delete(k) => sender() ! doDelete(k)
  }

  private[mws] def doPut(k: Key, v: Value): String = {
    log.info(s"[hash_ring: ${local.port}] put ($k->$v) on node ${cluster.selfAddress}")
    val bucket = hashing.findBucket(Left(k))
    val nodes = findNodes(Left(k))

    if (nodes.contains(local)) {
      log.info("[hash_ring]Process put on local node")
      val storeRef = system.actorSelection("/user/ring_store")
      val f = storeRef ? StoreGet(k)
      val data = Await.result(f, timeout.duration).asInstanceOf[Option[Data]]

      val vc: VectorClock = data match {
        case None => new VectorClock
        case Some(d) => d._4
      }
      val updatedData = (k, bucket, System.currentTimeMillis(), //TODO hack for lm time
        vc.:+(local.toString),
        hashing.digest(v.getBytes).mkString, "0", v)

      mapInPut(nodes, updatedData)
    } else {
      log.info(s"[hash_ring]Route to cluster nodes: $nodes")
      routeToPutCluster(nodes, k, v)
    }
  }


  private[mws] def doGet(key: Key, client: ActorRef) = {
    val nodes = findNodes(Left(key))
    import context.dispatcher
      val resultsF = Future.traverse(availableNodesFrom(nodes))(node =>
        (system.actorSelection(RootActorPath(node) / "user" / "ring_store") ? StoreGet(key)).mapTo[Option[Data]].map(data => (data, node)))
        resultsF.map(l => system.actorSelection("/user/ring_gatherer") ! GatherGet(l, client))
  }

  private[mws] def doDelete(k: Key): String = {
    import context.dispatcher
    val nodes = findNodes(Left(k))
    if (nodes contains local) {
      println(s"[hash_ring]Delete from local k = $k")
      val deleteF = Future.traverse(availableNodesFrom(nodes))(n =>
        (system.actorSelection(RootActorPath(n) / "user" / "ring_store") ? StoreDelete(k)).mapTo[String])

      val result = Await.result(deleteF, timeout.duration)
      if ((result contains "ok") && (result.size >= W)) {
        "ok"
      } else {
        "error"
      }

    } else {
      log.info(s"[hash_ring]Delete k = $k route to cluster")
      routeDeleteToCluster(nodes, k)
    }
  }

  @tailrec
  private def routeDeleteToCluster(nodes: List[Node], k: Key): String = nodes match {
    case Nil => "error"
    case head :: tail =>
      val delF = (system.actorSelection(RootActorPath(head) / "user" / "ring_hash") ? Delete(k)).mapTo[String]
      Await.result(delF, timeout.duration) match {
        case "ok" => "ok"
        case _ => routeDeleteToCluster(tail, k)
      }
  }

  def receiveCl: Receive = {
    case e:ClusterDomainEvent => cluster.sendCurrentClusterState(self) // TODO bind to another event to avoid unnecessary spam.
      e match {
      case MemberUp(member) if member.hasRole(rngRoleName) =>
        log.info(s"=>[ring_hash] Node ${member.address} is joining ring")
        (1 to vNodesNum).foreach(vnode => {
          val hashedKey = hashing.hash(Left(member.address, vnode))
          vNodes += hashedKey -> member.address})
        syncBuckets(updateBuckets)
      case MemberRemoved(member, prevState) if member.hasRole(rngRoleName) =>
        log.info(s"[ring_hash]Removing $member from ring")
        val hashes = (1 to vNodesNum).map(v => hashing.hash(Left((member.address, v))))
        vNodes = vNodes.filterNot(virtualNode => hashes.contains(virtualNode._1))
        syncBuckets(updateBuckets)
      case _ =>
      }
    case s: CurrentClusterState =>
      state = s
  }

  @tailrec
  private def routeToPutCluster(nodes: List[Node], k: Key, v: Value): String = nodes match {
    case head :: tail => {
      log.info(s"[hash_ring:${local.port} ]RoutPutToCluster: try to put in $head.")
      val path = RootActorPath(head) / "user" / "ring_hash"
      val hs = system.actorSelection(path)
      val hashPutF = (hs ? Put(k, v)).mapTo[String]
      Await.result(hashPutF, timeout.duration) match {
        case "ok" => "ok"
        case _ => routeToPutCluster(tail, k, v)
      }
    }
    case _ =>
      log.info(s"[hash:${local.port} ]RoutPutToCluster: no nodes to route")
      "error"
  }

  private def availableNodesFrom(l: List[Node]) = {
    val unreachableMembers = state.unreachable.map(m => m.address)
    l filterNot (node => unreachableMembers contains node)
  }

  private[mws] def mapInPut(nodes: List[Node], updatedData: Data): String = {
    import context.dispatcher
    val putFutures = Future.traverse(availableNodesFrom(nodes))(n =>
      (system.actorSelection(RootActorPath(n) / "user" / "ring_store") ? StorePut(updatedData)).mapTo[String])

    val result: List[String] = Await.result(putFutures, timeout.duration)
    log.debug(s"[hash:${local.port}] PutInMap: result = ${result} with configured W=$W")
    if ((result map (status => status.equals("ok")) size) >= W) "ok" else "error"
  }

  def updateBuckets(): List[HashBucket] = {
    val maxSearch = if (nodesInRing == 1) 1 else vNodes.size // don't search other nodes to fill the bucket when 1 node
    updateBuckets(bucketsNum - 1, maxSearch, List.empty)
  }

  @tailrec
  private def updateBuckets(b: Bucket, max: Int, replaced: List[HashBucket]): List[HashBucket] = b match {
    case -1 => replaced
    case bucket: Int =>
      val newNodes = searchBucketNodes(bucket * hashing.bucketRange, max, nodesInRing, Nil)
      buckets.get(bucket) match {
        case Some((`bucket`, `newNodes`)) =>
          updateBuckets(bucket - 1, max, replaced)
        case outdateBucket =>
          buckets += bucket ->(bucket, newNodes)
          val newReplica = newNodes.indexOf(cluster.selfAddress) match {
            case -1 => None
            case i: Int => Some(i)
          }
          val oldReplica: Option[Int] = outdateBucket match {
            case Some((_, oldNodes: List[Address])) =>
              oldNodes.indexOf(cluster.selfAddress) match {
                case -1 => None
                case i: Int => Some(i)
              }
            case _ => None
          }

          val replacedUpd = (oldReplica, newReplica) match {
            case (_, `oldReplica`) => replaced
            case _ => (bucket, newReplica, oldReplica) :: replaced
          }
          updateBuckets(bucket - 1, max, replacedUpd)
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
      case Some((`bucket`, nds)) => nds
      case _ => Nil
    }
  }

  @tailrec
  private def syncBuckets(buckets: List[HashBucket]): Unit = buckets match {
    case Nil => // done synch
    case (bucket, newReplica, oldReplica) :: tail =>
      (newReplica, oldReplica) match {
        case (`newReplica`, None) =>
          doUpdateBuckets(bucket, findNodes(Right(bucket)).filterNot(_ == cluster.selfAddress))
        case (None, `oldReplica`) =>
          deleteBucket(Left(bucket)) // TODO review
        case _ => //nop
      }
      syncBuckets(tail)
  }

  @tailrec
  private def deleteBucket(b: Either[Bucket, List[_]]): Unit = b match {
    case Right(Nil) => //ok
    case Right(metadata :: tail) =>
      deleteBucket(Right(tail))
    case Left(b) =>
      deleteBucket(Right(Nil))
  }

  @tailrec
  private def doUpdateBuckets(bucket: Bucket, nodes: List[Address]): Unit = {
    nodes match {
      case Nil =>
      case head :: tail =>
        val listf = system.actorSelection(RootActorPath(head) / "user" / "ring_store").?(StoreListBucket(bucket))
        val list = Await.result(listf, timeout.duration)
        list match {
          case list: List[Data] =>
            retrieveData(head, list)
          case ("error", reason) => // TODO - match real possible results
            doUpdateBuckets(bucket, tail)
        }
    }
  }

  @tailrec
  private def retrieveData(node: Address, list: List[Data]): (String, String) = {
    list match {
      case Nil => ("ok", "ok")
      case metadata :: rest =>
        val store = system.actorSelection("/user/ring_store")
        val dataf = store ? StoreGet(metadata._1)
        val data = Await.result(dataf, timeout.duration)

        data match {
          case Nil =>
            val store = system.actorSelection(RootActorPath(node) / "user" / "ring_store")
            val localStore = system.actorSelection("/user/ring_store")

            val getf = store ? StoreGet(metadata._1)
            val get = Await.result(getf, timeout.duration)

            get match {
              case list: List[Data] =>
                list.map(localStore ! StorePut(_))
                retrieveData(node, rest)
              case "undefined" =>
                retrieveData(node, rest)
              case ("error", reason) => // TODO process unsuccessful cases.
                log.info(s"retrieve data error $reason")
                ("error", reason.toString)
            }
          case d =>
            retrieveData(node, rest)
        }
    }
  }
}
