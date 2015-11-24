package mws.kvs

import mws.rng.Ack

import scala.concurrent.Future

trait Kvs {
  def put(key: String, str: AnyRef): Future[Ack]
  def get[T](key: String, clazz: Class[T]): Future[Option[T]]
  def delete(key: String): Future[Ack]
}
