package zd.kvs
package store

import scala.concurrent.Future
import zd.proto.Bytes

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
  def load(path: String): Res[Any]
  def onReady(): Future[Unit]
  def nextid(fid: String): Res[String]
  def compact(): Unit
  def clean(keyPrefix: Bytes): Res[Unit]
}
