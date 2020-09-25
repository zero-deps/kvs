package zd.kvs
package store

import scala.concurrent.Future
import zd.proto.Bytes

/**
 * Database Application Interface.
 */
trait Dba {
  def get(key: Bytes): Res[Option[Bytes]]
  def put(key: Bytes, value: Bytes): Res[Unit]
  def delete(key: Bytes): Res[Unit]

  def save(path: String): Res[String]
  def load(path: String): Res[Any]

  def onReady(): Future[Unit]
  def compact(): Unit
}
