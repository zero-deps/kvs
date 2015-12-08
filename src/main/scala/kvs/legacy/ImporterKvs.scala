package mws.kvs

import akka.actor.Actor
import java.util.Calendar

/*object ImporterKvs {
  case class Get(filePath: String)
  case class GetAck(importTime: Option[String], filePath: String)
  case class Put(filePath: String)
  case class PutAck(filePath: String)
}

trait ImporterKvs { _: Kvs with Actor =>
  import ImporterKvs._
  import scala.concurrent.ExecutionContext.Implicits.global
  
  def receive: Receive = {
    
    case Get(filePath) => {
      val s = sender()
      get(filePath, classOf[String]) onSuccess {
        case value => s ! GetAck(value, filePath)
      }
    }
      
    case Put(filePath) =>
      put(filePath, Calendar.getInstance().getTime.toString)
      sender ! PutAck(filePath)
  }
}
*/