package mws.rng

import akka.actor._
import akka.cluster.VectorClock
import scala.concurrent.duration._

class GatherGetFsm(client: ActorRef, N: Int, R: Int, k: Key)
  extends FSM[FsmState, FsmData] with ActorLogging{
  val stores = SelectionMemorize(context.system)
  val ZERO:(VectorClock, Long) = (new VectorClock(), 0L)

  startWith(Collecting, DataCollection(List.empty[(Option[Data], Node)], 0))
  setTimer("send_by_timeout", OpsTimeout, context.system.settings.config.getInt("ring.gather-timeout").seconds)

  when(Collecting) {
    case Event(GetResp(rez), state@DataCollection(perNode, nodes)) =>
      val address = sender().path.address
      val receive = rez.fold[List[(Option[Data], Node)]](List((None, address)))(_.map(d => (Some(d), address)))

      nodes + 1 match {
        case `N` => // TODO wait for R or N nodes ?
          cancelTimer("send_by_timeout")
          val response = order[(Option[Data], Node)](receive ::: perNode, age)
          if (response._2.nonEmpty){ response._1.foreach(freshData => update(freshData._1, response._2.map(_._2))) }
          client ! response._1.fold[Option[Value]](None)(d => d._1.fold[Option[Value]](None)(v => Some(v.value)))
          stop()
        case ns => stay() using DataCollection(receive ::: perNode, ns)
      }

    case Event(OpsTimeout, DataCollection(l, ns)) =>
      val response = order(l, age)
      client ! response._1.fold[Option[Value]](None)(d => d._1.fold[Option[Value]](None)(v => Some(v.value)))
      // cannot fix inconsistent because quorum not satisfied. But check what can do
      cancelTimer("send_by_timeout")
      stop()
  }

  def update(correct: Option[Data], nodes: List[Node]) = {
    val msg = correct match {
      case Some(d) => StorePut(d)
      case None => StoreDelete(k)
    }
    nodes foreach {
      stores.get(_, "ring_write_store").fold(_ ! msg, _ ! msg)
    }
  }

  def age(d: (Option[Data], Node)): (VectorClock, Long) = {
    d._1.fold(ZERO)(e => (e.vc, e.lastModified))
  }
}
