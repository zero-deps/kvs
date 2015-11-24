package mws.kvs
package store
import scala.concurrent.Future

trait Store {
  def put(key: String, str: String): Future[AnyVal]
  def get(key: String): Option[String]
  def delete(key: String): Unit
  def isReady: Future[Boolean]
  def close(): Unit
}
