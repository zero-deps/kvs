package mws.rng

import akka.actor.{ActorRef, FSM, Actor}
import akka.cluster.VectorClock

//api
sealed trait FeedAPI
case class AddToFeed(fid: String, v: Value, nodes: Seq[Node]) extends FeedAPI
case class TraverseFeed(fid: String, nodes: Seq[Node], start: Option[Int], end: Option[Int]) extends FeedAPI

//TODO add timeout
//TODO extract fid to field because one feed handlers by one worker
class FeedsSupervisor(stores: SelectionMemorize) extends Actor {

  override def receive: Receive = {
    case msg@AddToFeed =>
    case msg@TraverseFeed =>
  }
}

//workers

case class WorkerData(msg: Option[FeedAPI], vs: List[(Node, VectorClock)], updated: List[Node], client: Option[ActorRef])

sealed trait FeedWorkerState
case object CollectingVersions extends FeedWorkerState
case object WaitingStatuses extends FeedWorkerState
case object Idle extends FeedWorkerState

//TODO time out, rollback, read quorum from config
//TODO return Id

class AddWorker( stores: SelectionMemorize) extends FSM[FeedWorkerState, WorkerData] {
  startWith(CollectingVersions, WorkerData(None, Nil, Nil, None))

  when(Idle) {
    case Event(msg:AddToFeed, state) =>
      msg.nodes foreach (stores.get(_, "ring_readonly_store").fold(
        _.tell(StoreGet(s"${msg.fid}:version"), self),
        _.tell(StoreGet(s"${msg.fid}:version"), self)
      ))
      goto(CollectingVersions) using WorkerData(Some(msg), Nil, Nil, Some(sender()))
  }

  when(CollectingVersions) {
    case Event(GetResp(Some(d)), state) =>
      val s = WorkerData(state.msg, (sender.path.address, d.head.vc) :: state.vs, Nil, state.client)
      s.msg match  {
        case Some(AddToFeed(fid, v, nodes)) if nodes.size == s.vs.size => nodes foreach(
          stores.get(_, "ring_write_store").fold(
            _.tell(FeedAppend(fid, v, s.vs.head._2 :+ self.path.address.toString), self),
            _.tell(FeedAppend(fid, v, s.vs.head._2 :+ self.path.address.toString), self))
            )
          goto(WaitingStatuses) using s
        case Some(m) => stay() // TODO update outdated nodes
        case None => stay() using s //TODO stop and notify client about fail
      }
  }

  when(WaitingStatuses){
    case Event("ok", state) =>
      sender().path.address :: state.updated match {
        case l if l.size == 3 =>
          state.client foreach (_ ! "Added")
          goto(Idle) using WorkerData(None, Nil, Nil, None)
        case l => stay() using WorkerData(None, Nil, l, state.client)
      }
  }
  initialize()
}

class TraverseWorker(stores: SelectionMemorize) extends FSM[FeedWorkerState, WorkerData]{
  startWith(Idle, WorkerData(None, Nil,Nil,None))

  when(Idle){
    case Event(msg:TraverseFeed, state) =>
      msg.nodes foreach (stores.get(_, "ring_readonly_store").fold(
        _.tell(StoreGet(s"${msg.fid}:version"), self),
        _.tell(StoreGet(s"${msg.fid}:version"), self)
      ))
      goto(CollectingVersions) using WorkerData(Some(msg), Nil, Nil, Some(sender()))
  }

  when(CollectingVersions) {
    case Event(GetResp(Some(d)), state) =>
      (sender.path.address, d.head.vc) :: state.vs match {
        case l if l.size < 2 =>
          stay() using WorkerData(state.msg, (sender().path.address, d.head.vc) :: state.vs, Nil, state.client)
        case l => l find(v => v._1 == self.path.address) match {
          case Some(local) if l forall( v => v._2 == local._2 || v._2 < local._2) =>
            stores.get(self.path.address, "rng_readonly_store").fold(
            _.tell(state.msg, state.client.get),
            _.tell(state.msg, state.client.get))
            goto(Idle) using WorkerData(None, Nil, Nil, None)
          case None if l.size < 3 =>
            stay() using WorkerData(state.msg, (sender().path.address, d.head.vc) :: state.vs, Nil, state.client)
          case _ => // TODO update old nodes
            stores.get(l.head._1, "rng_readonly_store").fold(
              _.tell(state.msg, state.client.get),
              _.tell(state.msg, state.client.get))
            goto(Idle) using WorkerData(None, Nil, Nil, None)
        }
      }
  }

  initialize()
}

