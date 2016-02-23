package mws.rng

import mws.rng.store.{GetResp, FeedAppend, StoreGet}
import scala.concurrent.duration._
import akka.actor._
import akka.cluster.VectorClock
import mws.rng.BucketGuard.{GuardData, GuardState}

object BucketGuard{
  val ZERO_VECTOR_CLOCK = new VectorClock()
  sealed trait GuardState
  sealed trait GuardData
}
class BucketGuard(bid: String, nodes: List[Node]) extends FSM[GuardState,GuardData] with ActorLogging{
  val add = context.system.actorOf(Props(classOf[AddWorker], nodes, bid), "add_wrkr")
  val travers = context.system.actorOf(Props(classOf[TraverseWorker], nodes,bid), "trv_wrkr")

  override def receive: Receive = {
    case msg :Add =>
      log.info(s"Superviser $msg")
      add.tell( msg,sender())
    case msg: Traverse =>
      log.info(s"Superviser $msg")
      travers.tell( msg,sender())
    case e => log.warning(s"unexpected msg $e")
  }
}

//workers
case class WorkerData(msg: Option[RingMessage], versions: List[(Node, Option[VectorClock])], nodes: List[Node],
                      client: Option[ActorRef], updated: List[Node])

sealed trait FeedWorkerState
case object CollectingVersions extends FeedWorkerState
case object WaitingStatuses extends FeedWorkerState
case object Idle extends FeedWorkerState

class AddWorker(bid:String, ns: List[Node]) extends FSM[FeedWorkerState, WorkerData] with ActorLogging {
  val c = context.system.settings.config.getIntList("ring.quorum")
  val stores = SelectionMemorize(context.system)
  val N = c.get(0)
  val t = context.system.settings.config.getInt("ring.gather-timeout")

  startWith(Idle, WorkerData(None, Nil, ns, None, Nil))
  setTimer("add_timeout", OpsTimeout, t.seconds )

  when(Idle) {
    case Event(msg:Add, state) =>
      state.nodes foreach (stores.get(_, "ring_readonly_store").fold(
        _.tell(StoreGet(s"${msg.bid}:version"), self),
        _.tell(StoreGet(s"${msg.bid}:version"), self)
      ))
      goto(CollectingVersions) using WorkerData(Some(msg), Nil, state.nodes, Some(sender()),Nil)
  }

  when(CollectingVersions) {
    case Event(GetResp(d), state) =>
      val vc = d match {
        case None => None
        case Some(data) => Some(data.head.vc)
      }
      val updState = WorkerData(state.msg, (sender.path.address, vc) :: state.versions, state.nodes, state.client, Nil)
      updState.msg match  {
        case Some(Add(`bid`, v)) if state.nodes.size == updState.versions.size =>
          val newVc = updState.versions.flatMap(_._2) match {
            case Nil => new VectorClock().:+(self.path.address.toString)
            case l => l.head.:+(self.path.address.toString) // **** need update old
          }
          state.nodes foreach(
          stores.get(_, "ring_write_store").fold(
            _.tell(FeedAppend(bid, v,newVc), self),
            _.tell(FeedAppend(bid, v, newVc), self))
            )
          goto(WaitingStatuses) using updState
        case Some(m) => stay() // TODO update outdated nodes
        case None => stay() using updState //TODO stop and notify client about fail
      }
  }

  when(WaitingStatuses){
    case Event(id:Int, state) =>
      sender.path.address :: state.updated match {
        case l if l.size == N =>
          state.client foreach (_ ! id)
          goto(Idle) using WorkerData(None, Nil, state.nodes, None, Nil)
        case l => stay() using WorkerData(None, Nil,state.nodes , state.client, l)
      }
  }
  initialize()
}

class TraverseWorker(stores: SelectionMemorize, ns: List[Node]) extends FSM[FeedWorkerState, WorkerData] with ActorLogging{
  import  BucketGuard._
  val c = context.system.settings.config.getIntList("ring.quorum")
  val N = c.get(0)
  val R = c.get(2)
  startWith(Idle, WorkerData(None, Nil, ns, None, Nil))

  when(Idle){
    case Event(msg:Traverse, state) =>
      println(s"TraverseFeed WORKER $msg")
      state.nodes foreach (stores.get(_, "ring_readonly_store").fold(
        _.tell(StoreGet(s"${msg.bid}:version"), self),
        _.tell(StoreGet(s"${msg.bid}:version"), self)
      ))
      goto(CollectingVersions) using WorkerData(Some(msg), Nil, state.nodes, Some(sender()), Nil)
  }

  when(CollectingVersions) {
    case Event(GetResp(d), state) =>
      val version: Option[VectorClock] = d match {
        case None => None
        case Some(l) => Some(l.head.vc)
      }
      (sender.path.address, version) :: state.versions match {
        case l if l.size < R =>
          stay() using WorkerData(state.msg, l, state.nodes, state.client, Nil)
        case l => l find(v => v._1 == self.path.address) match {
          case Some(local) if l forall( v => v._2.getOrElse(ZERO_VECTOR_CLOCK) == local._2.getOrElse(ZERO_VECTOR_CLOCK)
            || v._2.getOrElse(ZERO_VECTOR_CLOCK) < local._2.getOrElse(ZERO_VECTOR_CLOCK)) =>
            stores.get(self.path.address, "ring_readonly_store").fold(
            _.tell(state.msg.get, state.client.get),
            _.tell(state.msg.get, state.client.get))
            goto(Idle) using WorkerData(None, Nil, state.nodes, None, Nil)
          case None if l.size < N =>
            stay() using WorkerData(state.msg, (sender().path.address, version) :: state.versions, state.nodes, state.client, Nil)
          case _ => // TODO update old nodes
            state.msg foreach(trav =>
              stores.get(l.head._1, "ring_readonly_store").fold(
              _.tell(trav, state.client.get),
              _.tell(trav, state.client.get))
              )
            goto(Idle) using WorkerData(None, Nil, state.nodes, None, Nil)
        }
      }
  }

  initialize()
}

