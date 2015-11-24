package mws.kvs

import akka.actor.{Actor, ActorLogging}
import akka.serialization.SerializationExtension
import akka.util.ByteString
import mws.rng.HashRing
import scala.concurrent.Future

trait RingKvs extends Kvs with Actor with ActorLogging {
  import scala.concurrent.ExecutionContext.Implicits.global
  
  val ring: HashRing = HashRing(context.system)
  val s = SerializationExtension(context.system)
  val schemaName: String

  override def put(key: String, v: AnyRef) = ring.put(composeKey(key), ByteString(s.serialize(v).get))

  override def get[T](key: String, clazz: Class[T]): Future[Option[T]] = {
    ring.get(composeKey(key)) map {
        case Some(v) => Some(s.deserialize(v.toArray, clazz).get)
        case None => None
    }
  }
  
  override def delete(key: String) = ring.delete(composeKey(key))

  private def composeKey(k: String): String = (schemaName, k).toString()
}
