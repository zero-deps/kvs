package zd.kvs
package store

import scala.concurrent.Future

/**
 * Database Application Interface.
 */
trait Dba {
  type K = String
  type V = Array[Byte]
  def put(key: K, value: V): Res[V]
  def get(key: K): Res[Option[V]]
  def delete(key: K): Res[V]
  def save(path: String): Res[String]
  def load(path: String): Res[Any]
  def loadJava(path: String): Res[Any]
  def iterate(path: String, f: (K, V) => Option[(K, V)], afterIterate: () => Unit): Res[Any]
  def isReady: Future[Boolean]
  def nextid(fid: String): Res[String]
  def compact(): Unit
}
