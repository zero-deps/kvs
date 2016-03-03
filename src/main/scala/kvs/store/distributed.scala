package mws.kvs
package store

import java.util.concurrent.TimeUnit
import akka.actor.ExtendedActorSystem
import akka.util.ByteString
import mws.rng.{HashRing, AckSuccess, Ack}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import com.typesafe.config.Config

object Ring {
  val not_found:Err = Dbe(msg="not_found")
  def apply(system: ExtendedActorSystem): Dba = new Ring(system)
}

class Ring(system: ExtendedActorSystem) extends Dba {
  import Ring._

  val rng = HashRing(system)

  val d = Duration(5, TimeUnit.SECONDS)

  override def put(key: String, value: V): Either[Err, V] = {
    Await.result(rng.put(key, ByteString(value)), d) match {
      case AckSuccess => Right(value)
      case not_success: Ack => Left(Dbe(msg = not_success.toString))
    }
  }
  override def isReady: Future[Boolean] = rng.isReady

  override def get(key: String): Either[Err, V] = Await.result(rng.get(key),d) match {
    case Some(v) => Right(v.toArray)
    case None => Left(not_found)
  }

  override def delete(key: String): Either[Err, V] = get(key) match {
    case value@ Right(v) => Await.result(rng.delete(key),d) match {
      case AckSuccess => value
      case not_success => Left(Dbe(msg = not_success.toString))
    }
    case err@ Left(msg) => err
  }

  override def save(): Unit = rng.dump()

  override def load(path: String): Unit = rng.load(path)

  override def close(): Unit = println("okay")
}
