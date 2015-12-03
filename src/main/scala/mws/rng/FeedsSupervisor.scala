package mws.rng

import akka.actor.{ActorRef, FSM, Actor}
import akka.cluster.VectorClock

case class AddToFeed(fid: String, v: Value, nodes: Seq[Node])
//TODO add timeout
class FeedsSupervisor(stores: SelectionMemorize) extends Actor {

  override def receive: Receive = {
    case msg@AddToFeed =>
  }
}

//workers

case class WorkerData(msg: Option[AddToFeed], vs: List[(Node, VectorClock)],updated: List[Node], client: Option[ActorRef])

sealed trait FeedWorkerState
case object CollectingVersions extends FeedWorkerState
case object WaitingStatuses extends FeedWorkerState
case object Idle extends FeedWorkerState

class AddWorker(stores: SelectionMemorize) extends FSM[FeedWorkerState, WorkerData] {
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
        case Some(m) if m.nodes.size == s.vs.size => m.nodes foreach(
          stores.get(_, "ring_write_store").fold(
            _.tell(FeedAppend(m.fid, m.v, s.vs.head._2.:+(self.path.address.toString)), self),
            _.tell(FeedAppend(m.fid, m.v, s.vs.head._2.:+(self.path.address.toString)), self))
            )
          goto(WaitingStatuses) using s
        case Some(m) => stay() // TODO update outdated nodes
        case None => stay() using s //TODO stop and notify client about fail
      }
  }

  when(WaitingStatuses){
    case Event("ok", state) =>
      sender().path.address :: state.updated match {
        case l if l.size == 3 => state.client foreach (_! "Added")
        case l => stay() using WorkerData(None, Nil, l, state.client)
      }
      goto(Idle)
  }

  initialize()
}

