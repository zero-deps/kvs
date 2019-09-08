package zd.kvs
package store

import scala.concurrent.Future

/**
 * Database Application Interface.
 */
trait Dba {
  def put(key: Bytes, value: Bytes): Res[Unit]
  def get(key: Bytes): Res[Option[Bytes]]
  def delete(key: Bytes): Res[Unit]
  def save(path: String): Res[String]
  def load(path: String): Res[Any]
  def isReady: Future[Boolean]
  def compact(): Unit
}
