package mws.rng

import akka.actor._
import akka.cluster.VectorClock

object FeedsSupervisor{
  val ZERO_VECTOR_CLOCK = new VectorClock()
}
//api
sealed trait FeedAPI
case class AddToFeed(fid: String, v: Value, nodes: Seq[Node]) extends FeedAPI
case class TraverseFeed(fid: String, nodes: Seq[Node], start: Option[Int], end: Option[Int]) extends FeedAPI

//TODO add timeout
//TODO extract fid and nodes to field ecause one feed handlers by one worker
class FeedsSupervisor(stores: SelectionMemorize) extends Actor with ActorLogging{
  val add = context.system.actorOf(Props(classOf[AddWorker],stores), "add_wrkr")
  val travers = context.system.actorOf(Props(classOf[TraverseWorker],stores), "trv_wrk")

  override def receive: Receive = {
    case msg :AddToFeed =>
      log.info(s"Superviser $msg")
      add.tell( msg,sender())
    case msg: TraverseFeed =>
      log.info(s"Superviser $msg")
      travers .tell( msg,sender())
    case e => log.info(s"unexpected msg $e")
  }
}

//workers

case class WorkerData(msg: Option[FeedAPI], vs: List[(Node, Option[VectorClock])], updated: List[Node], client: Option[ActorRef])

sealed trait FeedWorkerState
case object CollectingVersions extends FeedWorkerState
case object WaitingStatuses extends FeedWorkerState
case object Idle extends FeedWorkerState

//TODO time out, rollback, read quorum from config
//TODO return Id

class AddWorker(stores: SelectionMemorize) extends FSM[FeedWorkerState, WorkerData] with ActorLogging {
  val c = context.system.settings.config.getIntList("ring.quorum")
  val N = c.get(0)

  startWith(Idle, WorkerData(None, Nil, Nil, None))

  when(Idle) {
    case Event(msg:AddToFeed, state) =>
      msg.nodes foreach (stores.get(_, "ring_readonly_store").fold(
        _.tell(StoreGet(s"${msg.fid}:version"), self),
        _.tell(StoreGet(s"${msg.fid}:version"), self)
      ))
      goto(CollectingVersions) using WorkerData(Some(msg), Nil, Nil, Some(sender()))
  }

  when(CollectingVersions) {
    case Event(GetResp(d), state) =>
      val vc = d match {
        case None => None
        case Some(data) => Some(data.head.vc)
      }
      val s = WorkerData(state.msg, (sender.path.address, vc) :: state.vs, Nil, state.client)
      s.msg match  {
        case Some(AddToFeed(fid, v, nodes)) if nodes.size == s.vs.size =>
          val newVc = s.vs.flatMap(_._2) match {
            case Nil => new VectorClock().:+(self.path.address.toString)
            case l => l.head.:+(self.path.address.toString)
          }
          nodes foreach(
          stores.get(_, "ring_write_store").fold(
            _.tell(FeedAppend(fid, v,newVc), self),
            _.tell(FeedAppend(fid, v, newVc), self))
            )
          goto(WaitingStatuses) using s
        case Some(m) => stay() // TODO update outdated nodes
        case None => stay() using s //TODO stop and notify client about fail
      }
  }

  when(WaitingStatuses){
    case Event(id:Int, state) =>
      sender.path.address :: state.updated match {
        case l if l.size == N =>
          state.client foreach (_ ! id)
          goto(Idle) using WorkerData(None, Nil, Nil, None)
        case l => stay() using WorkerData(None, Nil, l, state.client)
      }
  }
  initialize()
}

class TraverseWorker(stores: SelectionMemorize) extends FSM[FeedWorkerState, WorkerData] with ActorLogging{
  import  FeedsSupervisor._
  val c = context.system.settings.config.getIntList("ring.quorum")
  val N = c.get(0)
  val R = c.get(2)
  startWith(Idle, WorkerData(None, Nil,Nil,None))

  when(Idle){
    case Event(msg:TraverseFeed, state) =>
      println(s"TraverseFeed WORKER $msg")
      msg.nodes foreach (stores.get(_, "ring_readonly_store").fold(
        _.tell(StoreGet(s"${msg.fid}:version"), self),
        _.tell(StoreGet(s"${msg.fid}:version"), self)
      ))
      goto(CollectingVersions) using WorkerData(Some(msg), Nil, Nil, Some(sender()))
  }

  when(CollectingVersions) {
    case Event(GetResp(d), state) =>
      val version: Option[VectorClock] = d match {
        case None => None
        case Some(l) => Some(l.head.vc)
      }
      (sender.path.address, version) :: state.vs match {
        case l if l.size < R =>
          stay() using WorkerData(state.msg, l, Nil, state.client)
        case l => l find(v => v._1 == self.path.address) match {
          case Some(local) if l forall( v => v._2.getOrElse(ZERO_VECTOR_CLOCK) == local._2.getOrElse(ZERO_VECTOR_CLOCK)
            || v._2.getOrElse(ZERO_VECTOR_CLOCK) < local._2.getOrElse(ZERO_VECTOR_CLOCK)) =>
            stores.get(self.path.address, "ring_readonly_store").fold(
            _.tell(state.msg.get, state.client.get),
            _.tell(state.msg.get, state.client.get))
            goto(Idle) using WorkerData(None, Nil, Nil, None)
          case None if l.size < N =>
            stay() using WorkerData(state.msg, (sender().path.address, version) :: state.vs, Nil, state.client)
          case _ => // TODO update old nodes
            state.msg foreach(trav =>
              stores.get(l.head._1, "ring_readonly_store").fold(
              _.tell(trav, state.client.get),
              _.tell(trav, state.client.get))
              )
            goto(Idle) using WorkerData(None, Nil, Nil, None)
        }
      }
  }

  initialize()
}

