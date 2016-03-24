package mws.rng

import akka.actor._
import akka.cluster.ClusterEvent._
import akka.cluster.{Member, Cluster}
import akka.pattern.ask
import akka.util.Timeout
import mws.rng.store._
import scala.annotation.tailrec
import scala.collection.SortedMap
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.collection.JavaConversions._
import scala.collection.breakOut

sealed class RingMessage
//kvs
case class Put(k: Key, v: Value) extends RingMessage
case class Get(k: Key) extends RingMessage
case class Delete(k: Key) extends RingMessage
//feed
case class Add(bid: String, v: Value) extends RingMessage
case class Traverse(bid: String, start: Option[Int], end: Option[Int]) extends RingMessage
case class Remove(nb: String, v: Value) extends RingMessage
case class RegisterBucket(bid: String) extends RingMessage
//utilities
case object Ready
case object Init
case object Dump
case class LoadDump(dumpPath:String)
case class DumpComplete(path: String)

sealed trait HashRngState
case object Starting extends HashRngState
case object WeakReadonly extends HashRngState
case object Readonly extends HashRngState
//case object Ready extends HashRngState

case class HashRngData(nodes: Set[Member],
                  buckets: SortedMap[Bucket, PreferenceList],
                  vNodes: SortedMap[Bucket, Address],
                  feedNodes: SortedMap[FeedId, PreferenceList])

class Hash extends FSM[HashRngState, HashRngData] with ActorLogging {
  import context.system
  implicit val timeout = Timeout(5.second)

  val config = system.settings.config.getConfig("ring")
  log.info(s"Ring configuration: ")
  for (c <- config.entrySet()) {
    log.info(s"${c.getKey} = ${c.getValue.render()}")
  }

  val quorum = config.getIntList("quorum")
  val N: Int = quorum.get(0)
  val W: Int = quorum.get(1)
  val R: Int = quorum.get(2)
  val gatherTimeout = config.getInt("gather-timeout")
  val vNodesNum = config.getInt("virtual-nodes")
  val bucketsNum = config.getInt("buckets")
  val cluster = Cluster(system)
  val local: Address = cluster.selfAddress
  val hashing = HashingExtension(system)
  val actorsMem = SelectionMemorize(system)

  startWith(Starting, HashRngData(Set.empty[Member], SortedMap.empty[Bucket, PreferenceList],
                                 SortedMap.empty[Bucket, Address],SortedMap.empty[FeedId, PreferenceList]))
  
  override def preStart() = {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents, classOf[MemberUp], classOf[MemberRemoved])
  }

  when(Starting){
    case Event(MemberUp(member), data) => {
      val next = joinNodeToRing(member, data)
      goto(next._1) using(next._2)
    }
  }

  override def postStop(): Unit = cluster.unsubscribe(self)

 /* def ready = readApi orElse writeApi orElse receiveClusterEvent
  def preparing = notReadyApi orElse receiveClusterEvent

  def readApi: Receive = {
    case Get(k) => doGet(k, sender())
    case msg: Traverse => feedNodes(msg.bid).headOption foreach(n => actorsMem.get(n,s"${msg.bid}-guard").fold(
      _ ! msg, _ ! msg
    )) 
   case m: RegisterBucket =>
      log.info(s"[hash] register bucket ${m.bid}")
      if(feedNodes(m.bid).isEmpty)
        feedNodes = feedNodes + (m.bid -> nodesForKey(m.bid))

      feedNodes(m.bid).headOption foreach {
        case n if n == local =>
          log.info(s"[hash] spawn guard for ${m.bid}")
          system.actorOf(Props(classOf[BucketGuard], nodesForKey(m.bid)),s"${m.bid}-guard")
          sender() ! "ok"
        case n => actorsMem.get(n,"hash").fold( // head is guard
        _ ! m, _ ! m
      )}
    case Ready => sender() ! true
    case Dump => 
        log.info("START DUMP")
        context.become(readApi) // readonly untill end of dump
        system.actorOf(Props(classOf[DumpWorker], buckets, local)).tell(Dump, sender)
    case DumpComplete(path) => 
        log.info(s"dump in file $path")
        context.become(ready)
        
  }

  def writeApi: Receive = {
    case Put(k, v) => doPut(k, v, sender())
    case Delete(k) => doDelete(k, sender())
    case msg: Add => feedNodes(msg.bid).headOption foreach(n => actorsMem.get(n,s"${msg.bid}-guard").fold(
      _ ! msg, _ ! msg
    ))
    case Ready => sender() ! true
    case m@LoadDump(dumpPath) => system.actorOf(Props(classOf[LoadDumpWorker], dumpPath)) ! m
  }

  def notReadyApi: Receive =  {
    case Ready => sender ! false
    case msg:RingMessage => log.info(s"ignoring $msg because ring is not ready")
  }

  def doPut(k: Key, v: Value, client: ActorRef):Unit = {
    val bucket = hashing findBucket k
    val nodes = availableNodesFrom(nodesForKey(k))
    log.debug(s"[hash][put] put $k -> $v on $nodes")
    if (nodes.size >= W) {
      val info: PutInfo = PutInfo(k, v, N, W, bucket, local, nodes)
      val gather = system.actorOf(GatherPutFSM.props(client, gatherTimeout, actorsMem, info))
      val node = nodes.find( _ == local).getOrElse(nodes.head)
        actorsMem.get(node, "ring_readonly_store").fold( _.tell(StoreGet(k), gather), _.tell(StoreGet(k), gather))
    } else {
      log.debug(s"[hash][put] put - quorum failed")
      client ! AckQuorumFailed
    }
  }

  def doGet(key: Key, client: ActorRef) : Unit = {
    val fromNodes = availableNodesFrom(nodesForKey(key))
    if (fromNodes.nonEmpty) {
      log.debug(s"[hash][get] k = $key from $fromNodes")
      val gather = system.actorOf(Props(classOf[GatherGetFsm], client, fromNodes.size, R, key))
      val stores = fromNodes map { actorsMem.get(_, "ring_readonly_store") }
      stores foreach (store => store.fold(
        _.tell(StoreGet(key), gather),
        _.tell(StoreGet(key), gather)))
    } else {
      log.debug(s"[hash][get] k = $key no nodes to get")
      client ! None
    }
  }

  def doDelete(k: Key, client: ActorRef): Unit = {
    import context.dispatcher
    val nodes = nodesForKey(k)
    val deleteF = Future.traverse(availableNodesFrom(nodes))(n =>
      (system.actorSelection(RootActorPath(n) / "user" / "ring_write_store") ? StoreDelete(k)).mapTo[String])
    deleteF.map(statuses => system.actorSelection("/user/ring_gatherer") ! GatherDel(statuses, client))
  }

  def receiveClusterEvent: Receive = {
    case MemberRemoved(member, prevState) =>
      processedNodes = processedNodes - member
      log.info(s"[ring_hash]Removing $member from ring")
      val hashes = (1 to vNodesNum).map(v => hashing.hash(member.address.hostPort + v))
      vNodes = vNodes.filterNot(vn => hashes.contains(vn._1))
      synchNodes(bucketsToUpdate(member.address))
    case _ =>
  }
*/
  def availableNodesFrom(l: List[Node]): List[Node] = {
    val unreachableMembers = cluster.state.unreachable.map(m => m.address)
    l filterNot (node => unreachableMembers contains node)
  }
  
  def joinNodeToRing(member: Member, data: HashRngData): (HashRngState, HashRngData) = {
      val newvNodes: Map[Bucket, Address] = (1 to vNodesNum).map(vnode => {
        hashing.hash(member.address.hostPort + vnode) -> member.address})(breakOut)
      val moved = bucketsToUpdate(bucketsNum - 1, if (data.nodes.size == 0) 1 else data.vNodes.size,
       data.nodes.size +1, data) // nodes of node +1?
      synchNodes(moved)
      val nextState = if(data.nodes.size +1 == N) Starting else Starting
      log.info(s"[rng] Node ${member.address} is joining ring. Nodes in ring = ${data.nodes.size +1}")
      
      (nextState, HashRngData(data.nodes + member, data.buckets++moved, data.vNodes ++ newvNodes, data.feedNodes))
  }

  @tailrec
  final def bucketsToUpdate(bucket: Bucket, max: Int, nodesCount: Int, data: HashRngData,
    changedPrefList: SortedMap[Bucket, PreferenceList] = SortedMap.empty[Bucket, PreferenceList]): SortedMap[Bucket, PreferenceList] = bucket match {
    case -1 => changedPrefList
    case bucket: Int =>
      val prefList = findBucketNodes(bucket * hashing.bucketRange, max, nodesCount, data)
      data.buckets(bucket) match {
        case `prefList` => // TODO may be Set ? List matching...
          bucketsToUpdate(bucket - 1, max, nodesCount, data, changedPrefList)
        case outdatedNodes =>
          bucketsToUpdate(bucket - 1, max, nodesCount, data, changedPrefList + (bucket -> prefList))
      }
  }

  @tailrec
  final def findBucketNodes(bucketRange: Int, maxSearch: Int, nodesAvailable: Int, data: HashRngData, 
    nodes: PreferenceList = Set.empty[Node]): PreferenceList = maxSearch match {
    case 0 => nodes
    case _ =>
      val it = data.vNodes.keysIteratorFrom(bucketRange)
      val hashedNode = if (it.hasNext) it.next() else data.vNodes.firstKey
      val node = data.vNodes(hashedNode)
      val prefList = if (nodes.contains(node)) nodes else nodes + node

      prefList.size match {
        case `N` => prefList
        case `nodesAvailable` => prefList
        case _ => findBucketNodes(hashedNode + 1, maxSearch - 1,nodesAvailable, data, prefList)
      }
  }

  /*def nodesForKey(k: Key): PreferenceList = buckets.get(hashing.findBucket(k)) match {
    case None => Nil
    case Some(nods) => nods
  }*/

  def synchNodes(buckets: SortedMap[Bucket, PreferenceList]): Unit = {
  buckets.foreach{bdata => 
    if(bdata._2.contains(local)) updateBucket(bdata._1, bdata._2.filterNot(_ == local))}
  }

  def updateBucket(bucket: Bucket, nodes: PreferenceList): Unit = {
    import context.dispatcher
    val storesOnNodes = nodes.map { actorsMem.get(_, "ring_readonly_store")}
    val bucketsDataF = Future.traverse(storesOnNodes)(n => n.fold(
      _ ? BucketGet(bucket),
      _ ? BucketGet(bucket))).mapTo[List[GetBucketResp]]

    bucketsDataF map {
      case  bdata: List[GetBucketResp] if bdata.isEmpty || bdata.forall(_.l == Nil) =>
      case  bdata: List[GetBucketResp] => 
        val put = mergeBucketData((List.empty[Data] /: bdata )((acc, resp) => resp.l.getOrElse(Nil) ::: acc), Nil)
        actorsMem.get(local, "ring_write_store").fold( _ ! BucketPut(put), _ ! BucketPut(put))
    }
  }

  initialize()
}
