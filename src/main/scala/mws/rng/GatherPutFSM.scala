package mws.rng

import akka.actor.{LoggingFSM, ActorLogging, ActorRef, FSM}
import scala.concurrent.duration._



class GatherPutFSM(val client: ActorRef, val W: Int) extends LoggingFSM[FsmState, FsmData] with ActorLogging{
  
  startWith(Collecting, Statuses(Nil), Some(2 seconds))
  setTimer("send_by_timeout", GatherTimeout, 2 seconds)
  
  when(Collecting) {
    case Event(incomeStatus: String, Statuses(statuses)) =>

      val updStatuses = Statuses( incomeStatus :: statuses )
      updStatuses.l.count(_ == "ok") match {
        case `W` =>
          client ! AckSuccess
          stop()
        case _ => stay using updStatuses
      }

    case Event(StateTimeout, _) =>
      client ! AckQuorumFailed
      stop()
  }
  
  initialize()
}
