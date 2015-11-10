package mws.kvs

import akka.actor.{ActorRef, Actor, ActorLogging}
import mws.kvs.TestKvs._
import collection.immutable

object TestKvs {
  case class Put(key: String, value: String)
  case class PutAck()
  case class Get(key: String)
  case class GetAck(value: Option[String])
  case class Delete(key: String)
  case class DeleteAck()
}

trait TestKvs extends Actor with ActorLogging { _: Kvs =>
  import scala.concurrent.ExecutionContext.Implicits.global
  
  def receive: Receive = {
    case Put(key, value) =>
      put(key, value)
      sender ! PutAck()
    case Get(key) =>
      val sender: ActorRef = context.sender
      get(key, classOf[String]) onSuccess {
        case v =>
          sender ! GetAck(v)
      }
    case Delete(key) =>
      delete(key)
      sender ! DeleteAck()
  }
}
