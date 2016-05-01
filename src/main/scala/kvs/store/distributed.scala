package mws.kvs
package store

import java.util.concurrent.TimeUnit
import scala.util._
import akka.actor.ExtendedActorSystem
import akka.util.ByteString
import mws.rng.{HashRing, AckSuccess, Ack}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import com.typesafe.config.Config

object Ring {
  def apply(system: ExtendedActorSystem): Dba = new Ring(system)
}

class Ring(system: ExtendedActorSystem) extends Dba {
  import Ring._

  val rng = HashRing(system)

  val d = Duration(5, TimeUnit.SECONDS)

  def put(key: String, value: V): Either[Err, V] =
    Try(Await.result(rng.put(key,ByteString(value)),d)) match {
      case Success(AckSuccess) => Right(value)
      case Success(not_success: Ack) => Left(Dbe(msg=not_success.toString))
      case Failure(ex) => Left(Dbe(msg=ex.getMessage))
    }

  def isReady: Future[Boolean] = rng.isReady

  def get(key: String): Either[Err, V] =
    Try(Await.result(rng.get(key),d)) match {
      case Success(Some(v)) => Right(v.toArray)
      case Success(None) => Left(Dbe(msg=s"not_found key $key"))
      case Failure(ex) => Left(Dbe(msg=ex.getMessage))
    }

  def delete(key: String): Either[Err, V] =
    get(key).fold(
      l => Left(l),
      r => Try(Await.result(rng.delete(key),d)) match {
        case Success(AckSuccess) => Right(r)
        case Success(not_success) => Left(Dbe(msg=not_success.toString))
        case Failure(ex) => Left(Dbe(msg=ex.getMessage))
      }
    )

  def save(): Unit = rng.dump()

  def load(path: String): Unit = rng.load(path)

  def close(): Unit = ()
}
