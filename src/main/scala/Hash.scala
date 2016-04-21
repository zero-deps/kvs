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
import scala.concurrent.Future
import scala.collection.JavaConversions._
import scala.collection.breakOut

sealed class APIMessage
//kvs
case class Put(k: Key, v: Value) extends APIMessage
case class Get(k: Key) extends APIMessage
case class Delete(k: Key) extends APIMessage
case object Dump extends APIMessage
case class LoadDump(dumpPath:String) extends APIMessage
case class DumpComplete(path: String) extends APIMessage
case object LoadDumpComplete extends APIMessage
//feed
case class Add(bid: String, v: Value) extends APIMessage
case class Traverse(bid: String, start: Option[Int], end: Option[Int]) extends APIMessage
case class Remove(nb: String, v: Value) extends APIMessage
case class RegisterBucket(bid: String) extends APIMessage
//utilities
case object Ready
case class ChangeState(s: QuorumState)
case class InternalPut(k: Key, v: Value)

sealed trait QuorumState
case object Unsatisfied extends QuorumState
case object Readonly extends QuorumState
case object Effective extends QuorumState
//  WriteOnly and WeakReadonly are currently ignored because rng is always readonly
case object WriteOnly extends QuorumState
case object WeakReadonly extends QuorumState

case class HashRngData(nodes: Set[Node],
                  buckets: SortedMap[Bucket, PreferenceList],
                  vNodes: SortedMap[Bucket, Node],
                  feedNodes: SortedMap[NamedBucketId, PreferenceList])
// TODO available/not avaiable nodes
class Hash extends FSM[QuorumState, HashRngData] with ActorLogging {
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

  startWith(Unsatisfied, HashRngData(Set.empty[Node], SortedMap.empty[Bucket, PreferenceList],
                                 SortedMap.empty[Bucket, Address],SortedMap.empty[NamedBucketId, PreferenceList]))

  override def preStart() = {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents, classOf[MemberUp], classOf[MemberRemoved])
  }
  override def postStop(): Unit = cluster.unsubscribe(self)

  when(Unsatisfied){
    case Event(ignoring: APIMessage, _) =>
      log.debug(s"Not enough nodes to process  ${cluster.state}: $ignoring")
      stay()
  }

  when(Readonly){
    case Event(Get(k), data) =>
      doGet(k, sender(), data)
      stay()
    case Event(DumpComplete(path), data) =>
      val new_state = state(data.nodes.size)
      data.nodes.foreach(n => actorsMem.get(n, "ring_hash").fold(_ ! ChangeState(new_state), _ ! ChangeState(new_state)))
      goto(new_state)
    case Event(msg:Traverse, data) =>
      data.feedNodes(msg.bid).headOption foreach(n => actorsMem.get(n,s"${msg.bid}-guard").fold(
        _ ! msg, _ ! msg
      ))
      stay()
    case Event(LoadDumpComplete, data) =>
      val s = state(data.nodes.size)
      data.nodes.foreach(n => actorsMem.get(n, "ring_hash").fold(_ ! ChangeState(s), _ ! ChangeState(s)))
      goto(s)
    case Event(Ready, _) => sender() ! false
      stay()
  }

  when(Effective){
    case Event(Ready, _) =>
      sender() ! true
      stay()
    case Event(Get(k), data) =>
      doGet(k, sender(), data)
      stay()
    case Event(Put(k,v), data) =>
      doPut(k,v,sender(),data)
      stay()
    case Event(Delete(k), data) =>
      doDelete(k,sender(),data)
      stay()
    case Event(Dump, data) =>
      system.actorOf(Props(classOf[DumpWorker], data.buckets, local)).tell(Dump, sender)
      data.nodes.foreach(n => actorsMem.get(n, "ring_hash").fold(_ ! ChangeState(Readonly), _ ! ChangeState(Readonly)))
      goto(Readonly)
    case Event(LoadDump(dumpPath), data) =>
      system.actorOf(Props(classOf[LoadDumpWorker], dumpPath)) ! LoadDump(dumpPath)
      data.nodes.foreach(n => actorsMem.get(n, "ring_hash").fold(_ ! ChangeState(Readonly), _ ! ChangeState(Readonly)))
      goto(Readonly)
      stay()
    case Event(RegisterBucket(bid), data) =>
      val feedNodes = registerNambedBucket(bid, data)
      stay() using HashRngData(data.nodes, data.buckets, data.vNodes, feedNodes)
    case Event(msg:Traverse, data) =>
      data.feedNodes(msg.bid).headOption foreach(n => actorsMem.get(n,s"${msg.bid}-guard").fold(
        _ ! msg, _ ! msg
      ))
      stay()
    case Event(msg:Add, data) =>
      data.feedNodes(msg.bid).headOption foreach(n => actorsMem.get(n,s"${msg.bid}-guard").fold(
        _ ! msg, _ ! msg
      ))
      stay()
  }

  /* COMMON FOR ALL STATES*/
  whenUnhandled {
    case Event(MemberUp(member), data) =>
      val next = joinNodeToRing(member, data)
      //log.info(s"[hash] MemberUp $member, next state= ${next._1}")
      goto(next._1) using next._2
    case Event(MemberRemoved(member, prevState), data) =>
      val next = removeNodeFromRing(member, data)
      goto(next._1) using next._2
    case Event(Ready, data) =>
      sender() ! false
      stay()
    case Event(ChangeState(s), data) =>
      if(state(data.nodes.size) == Unsatisfied) stay() else goto(s)
    case Event(InternalPut(k, v), data) =>
      doPut(k, v, self, data)
      stay()
  }

 def doDelete(k: Key, client: ActorRef, data: HashRngData): Unit = {
  import context.dispatcher
 val nodes = nodesForKey(k, data)
 val deleteF = Future.traverse(nodes)(n =>
  (system.actorSelection(RootActorPath(n) / "user" / "ring_write_store") ? StoreDelete(k)).mapTo[String])
  deleteF.map(statuses => system.actorSelection("/user/ring_gatherer") ! GatherDel(statuses, client))
}
 def doPut(k: Key, v: Value, client: ActorRef, data: HashRngData):Unit = {
   val bucket = hashing findBucket k
   val nodes = nodesForKey(k, data)
   log.debug(s"[hash][put] put $k -> (value lenght)= ${v.size} on $nodes")
   if (nodes.size >= W) {
     val info: PutInfo = PutInfo(k, v, N, W, bucket, local, data.nodes)
     val gather = system.actorOf(GatherPutFSM.props(client, gatherTimeout, actorsMem, info))
     val node = nodes.find( _ == local).getOrElse(nodes.head)
     actorsMem.get(node, "ring_readonly_store").fold( _.tell(StoreGet(k), gather), _.tell(StoreGet(k), gather))
   } else {
     log.debug(s"[hash][put] put - quorum failed")
     client ! AckQuorumFailed
   }
 }

 def doGet(key: Key, client: ActorRef, data: HashRngData) : Unit = {
   val fromNodes = nodesForKey(key, data)
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

  def availableNodesFrom(l: List[Node]): List[Node] = {
    val unreachableMembers = cluster.state.unreachable.map(m => m.address)
    l filterNot (node => unreachableMembers contains node)
  }

  def registerNambedBucket(bid: String, data: HashRngData): SortedMap[NamedBucketId,PreferenceList] = {
    log.info(s"[hash] register bucket $bid")
    val updFeedNodes = data.feedNodes + (bid -> nodesForKey(bid, data))
    updFeedNodes(bid).headOption foreach {
      case n if n == local =>
        log.info(s"[hash] spawn guard for $bid")
        system.actorOf(Props(classOf[BucketGuard], nodesForKey(bid, data), s"$bid-guard"))
        sender() ! "ok"
      case n => actorsMem.get(n, "hash").fold(// head is guard
        _ ! RegisterBucket(bid), _ ! RegisterBucket(bid)
      )
    }
    updFeedNodes
  }

  def joinNodeToRing(member: Member, data: HashRngData): (QuorumState, HashRngData) = {
      val newvNodes: Map[Bucket, Address] = (1 to vNodesNum).map(vnode => {
        hashing.hash(member.address.hostPort + vnode) -> member.address})(breakOut)
      val updvNodes = data.vNodes ++ newvNodes
      val nodes = data.nodes + member.address
      val moved = bucketsToUpdate(bucketsNum - 1, nodes.size, updvNodes, data.buckets)
      synchNodes(moved)
      val updData:HashRngData = HashRngData(nodes, data.buckets++moved,updvNodes , data.feedNodes)
      val ss = moved.foldLeft(Set.empty[Node])((acc,vn) => acc ++ vn._2)
      
      log.info(s"[rng] Node ${member.address} is joining ring. Nodes in ring = ${updData.nodes.size}, state = ${state(updData.nodes.size)}")
      (state(updData.nodes.size), updData)
  }

  def removeNodeFromRing(member: Member, data: HashRngData) : (QuorumState, HashRngData) = {
    log.info(s"[ring_hash]Removing $member from ring")
      val unusedvNodes: Map[Bucket, Address] = (1 to vNodesNum).map(vnode => {
      hashing.hash(member.address.hostPort + vnode) -> member.address})(breakOut)
      val updvNodes = data.vNodes.filterNot(vn => unusedvNodes.contains(vn._1))
      val nodes = data.nodes + member.address
      val moved = bucketsToUpdate(bucketsNum - 1, nodes.size, updvNodes, data.buckets)
      val updData: HashRngData = HashRngData(data.nodes + member.address, data.buckets++moved, updvNodes, data.feedNodes)
      synchNodes(moved)
      (state(updData.nodes.size), updData)
  }

  def state(nodes : Int): QuorumState = nodes match {
    case 0 => Unsatisfied
    case n if n >= Seq(R,W).max => Effective
    case _ => Readonly
  }

  def bucketsToUpdate(bucket: Bucket, nodesNumber: Int, vNodes: SortedMap[Bucket, Address],
                      buckets: SortedMap[Bucket, PreferenceList]): SortedMap[Bucket, PreferenceList] = {
    (0 to bucket).foldLeft(SortedMap.empty[Bucket, PreferenceList])((acc, b) => {
       val prefList = findBucketNodes(bucket * hashing.bucketRange, if (nodesNumber == 0) 1 else vNodes.size, vNodes, nodesNumber)
      
      buckets.get(b) match {
      case None => acc + (b -> prefList)
      case Some(`prefList`) => acc
      case _ => acc + (b -> prefList)
    }})
  }    

  @tailrec
  final def findBucketNodes(bucketRange: Int, maxSearch: Int, vNodes: SortedMap[Bucket, Address],
                            nodesNumber: Int, nodes: PreferenceList = Set.empty[Node]): PreferenceList =
  maxSearch match {
    case 0 => nodes
    case _ =>
        val it = vNodes.keysIteratorFrom(bucketRange)
        val hashedNode = if (it.hasNext) it.next() else vNodes.firstKey
        val node = vNodes(hashedNode)
        val prefList = if (nodes.contains(node)) nodes else nodes + node
        prefList.size match {
          case `N` => prefList
          case `nodesNumber` => prefList
          case _ => findBucketNodes(hashedNode + 1, maxSearch - 1, vNodes, nodesNumber, prefList)
        }
  }

  def nodesForKey(k: Key, data: HashRngData): PreferenceList = data.buckets.get(hashing.findBucket(k)) match {
    case None => Set.empty[Node]
    case Some(nods) => nods
  }

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
