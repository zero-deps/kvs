package mws.kvs
package store

import akka.actor.{Actor, ActorLogging}
import mws.rng.{AckSuccess, Ack}
import collection.concurrent.TrieMap
import scala.concurrent.Future

object Memory {
  def apply(): Kvs = new Memory
}
class Memory extends Kvs {
  val data = TrieMap[String, String]()

  def put(key: String, value: AnyRef): Future[Ack] = {
    data.put(key, value.toString)
    Future.successful(AckSuccess)
  }

  def get[T](key: String, clazz: Class[T]):  Future[Option[T]] = Future.successful(data.get(key) match {
    case Some(rez) => Some(rez.asInstanceOf[T])
    case _ => None
  })

  def delete(key: String): Future[Ack] = {
    data.remove(key)
    Future.successful(AckSuccess)
  }

  def isReady: Future[Boolean] = Future.successful(true)

  def close: Unit = println("close")
  def entries: Iterator[String] = Iterator.empty
  def add[T](container: String,el: T): Either[Throwable,T] = ???
  def remove[T](container: String,el: T): Either[Throwable,T] = ???
}
