package mws.rng

import akka.actor.{ActorRef, FSM}
import scala.concurrent.duration._



class GatherPutFSM(val client: ActorRef, val W: Int) extends FSM[FsmState, FsmData] {
  
  startWith(Collecting, Statuses(Nil), Some(2 seconds))  
  
  when(Collecting) {
    case Event(status: String, persisted:Statuses) =>
      
      val updStatuses = Statuses( status :: persisted.l )
      updStatuses.l.count(_ == "ok") match {
        case W => 
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
