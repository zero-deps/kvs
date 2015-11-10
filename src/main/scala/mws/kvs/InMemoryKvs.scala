package mws.kvs

import akka.actor.{Actor, ActorLogging}
import mws.rng.{AckSuccess, Ack}
import collection.concurrent.TrieMap
import scala.concurrent.Future

object InMemoryKvs{
  val map = TrieMap[String, AnyRef]()
  
  def clearDb = map.clear() // for test only
}

trait InMemoryKvs extends Kvs with Actor with ActorLogging {
  import InMemoryKvs._
  
  def put(key: String, str: AnyRef): Future[Ack] = {
    map.put(key, str)
    Future.successful(AckSuccess)
  }
  
  def get[T](key: String, clazz: Class[T]):  Future[Option[T]] = Future.successful(map.get(key) match {
    case Some(rez) => Some(rez.asInstanceOf[T])
    case _ => None
  })
  
  def delete(key: String): Future[Ack] = {
    map.remove(key)
    Future.successful(AckSuccess)
  }
}
