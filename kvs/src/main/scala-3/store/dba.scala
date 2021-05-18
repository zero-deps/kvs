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
  def delete(key: K): Res[Unit]
  def save(path: String): Res[String]
  def load(path: String): Res[String]
  def onReady(): Future[Unit]
  def nextid(fid: String): Res[String]
  def compact(): Unit
  def clean(keyPrefix: Array[Byte]): Res[Unit]
}
