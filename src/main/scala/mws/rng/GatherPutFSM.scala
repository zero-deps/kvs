package mws.rng

import akka.actor.{ActorLogging, ActorRef, FSM}
import scala.concurrent.duration._



class GatherPutFSM(val client: ActorRef, val N: Int, val W: Int, t: Int) extends FSM[FsmState, FsmData] with ActorLogging{
  
  startWith(Collecting, Statuses(Nil))
  setTimer("send_by_timeout", GatherTimeout, t seconds)
  
  when(Collecting) {
    case Event(incomeStatus: String, Statuses(statuses)) =>
      val updStatuses = Statuses( incomeStatus :: statuses )
      updStatuses.l.count(_ == "ok") match {
        case `W` =>
          client ! AckSuccess
          goto(Sent) using ReceivedValues(W)
        case _ => stay using updStatuses
      }

    case Event(GatherTimeout, _) =>
      client ! AckTimeoutFailed
      cancelTimer("send_by_timeout")
      stop()
  }
  
  when(Sent){
    case Event(status: String, ReceivedValues(n)) => 
      if(n + 1 == N)
        stop()
      else
        stay using ReceivedValues(n + 1)
    case Event(GatherTimeout, _ ) =>
      cancelTimer("send_by_timeout")
      stop()
  }
  
  initialize()
}
