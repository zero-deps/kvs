package zd.kvs

import scala.concurrent.Future

/** Database Application Interface */
trait Dba { self: AutoCloseable =>
  type K = String
  type V = Array[Byte]
  type R[A] = Either[Err, A]

  def put(key: K, value: V): R[Unit]
  def get(key: K): R[Option[V]]
  def delete(key: K): R[Unit]
  def nextid(fid: String): R[String]

  def save(path: String): R[String]
  def load(path: String): R[String]

  def onReady(): Future[Unit]
  def compact(): Unit
  def deleteByKeyPrefix(keyPrefix: K): R[Unit]

  def close(): Unit
}

extension (x: Dba)
  def getOrFail(key: x.K): x.R[x.V] =
    x.get(key).flatMap{
      case None => Left(KeyNotFound)
      case Some(x) => Right(x)
    }
