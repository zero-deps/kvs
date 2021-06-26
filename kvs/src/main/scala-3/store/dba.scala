package zd.kvs

import scala.concurrent.Future

/** Database Application Interface */
trait Dba:
  type K = String
  type V = Array[Byte]
  def put(key: K, value: V): Either[Err, V]
  def get(key: K): Either[Err, Option[V]]
  def getOrFail(key: K): Either[Err, V] =
    get(key).flatMap{
      case None => Left(KeyNotFound)
      case Some(x) => Right(x)
    }
  def delete(key: K): Either[Err, Unit]
  def save(path: String): Either[Err, String]
  def load(path: String): Either[Err, String]
  def onReady(): Future[Unit]
  def nextid(fid: String): Either[Err, String]
  def compact(): Unit
  def deleteByKeyPrefix(keyPrefix: Array[Byte]): Either[Err, Unit]
