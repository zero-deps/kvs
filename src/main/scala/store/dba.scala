package mws.kvs
package store

import scala.concurrent.Future

/**
 * Database Application Interface.
 */
trait Dba {
  type K = String
  type V = Array[Byte]
  def put(key: K, value: V): Res[V]
  def get(key: K): Res[V]
  def delete(key: K): Res[V]
  def save(path: String): Future[String]
  def load(path: String): Future[Any]
  def loadJava(path: String): Future[Any]
  def iterate(path: String, f: (K, V) => Unit): Future[Any]
  def isReady: Future[Boolean]
  def nextid(fid: String): Res[String]
}
