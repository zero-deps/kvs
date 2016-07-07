package mws.kvs
package store

import java.util.concurrent.TimeUnit
import scala.util._
import akka.actor.ExtendedActorSystem
import akka.util.ByteString
import mws.rng.{HashRing, AckSuccess, Ack}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object Ring {
  def apply(system: ExtendedActorSystem): Dba = new Ring(system)
}

class Ring(system: ExtendedActorSystem) extends Dba {
  val rng = HashRing(system)

  val d = Duration(5, TimeUnit.SECONDS)

  def put(key: String, value: V): Either[Err, V] =
    Try(Await.result(rng.put(key,ByteString(value)),d)) match {
      case Success(AckSuccess) => Right(value)
      case Success(not_success: Ack) => Left(not_success.toString)
      case Failure(ex) => Left(ex.getMessage)
    }

  def isReady: Future[Boolean] = rng.isReady

  def get(key: String): Either[Err, V] =
    Try(Await.result(rng.get(key),d)) match {
      case Success(Some(v)) => Right(v.toArray)
      case Success(None) => Left(s"not_found key $key")
      case Failure(ex) => Left(ex.getMessage)
    }

  def delete(key: String): Either[Err, V] =
    get(key).fold(
      l => Left(l),
      r => Try(Await.result(rng.delete(key),d)) match {
        case Success(AckSuccess) => Right(r)
        case Success(not_success) => Left(not_success.toString)
        case Failure(ex) => Left(ex.getMessage)
      }
    )

  def save():Future[String] = rng.dump()
  def load(path:String):Future[Any] = rng.load(path)
  def iterate(path:String,foreach:(String,Array[Byte])=>Unit):Future[Any] = rng.iterate(path,foreach)

  def close(): Unit = ()
}
