package mws.kvs
package store

import akka.actor.{ActorSystem}
import akka.serialization.SerializationExtension
import akka.util.ByteString
import mws.rng.HashRing
import scala.concurrent.Future

/*object Ring {
  def apply()(implicit system: ActorSystem): Kvs = new Ring
}

class Ring(implicit system: ActorSystem) extends Kvs {
  import scala.concurrent.ExecutionContext.Implicits.global
  val rng: HashRing = HashRing(system)
  val s = SerializationExtension(system)
  val schemaName: String = "s"

  def put(key: String, v: AnyRef) = rng.put(composeKey(key), ByteString(s.serialize(v).get))

  def get[T](key: String, clazz: Class[T]): Future[Option[T]] = {
    rng.get(composeKey(key)) map {
        case Some(v) => Some(s.deserialize(v.toArray, clazz).get)
        case None => None
    }
  }
  def delete(key: String) = rng.delete(composeKey(key))

  private def composeKey(k: String): String = (schemaName, k).toString()

  def isReady: Future[Boolean] = rng.isReady

  def close: Unit = println("close")
  def entries: Iterator[String] = Iterator.empty
  def add[T](container: String,el: T): Either[Throwable,T] = ???
  def remove[T](container: String,el: T): Either[Throwable,T] = ???

}
*/