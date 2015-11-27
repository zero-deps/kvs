package mws.kvs
package stats

import store._
import mws.rng.Ack
import com.typesafe.config.Config
import akka.actor.ActorSystem
import scala.concurrent.Future

class LastMetricKvs(container: String)(implicit val system:ActorSystem) extends Kvs with Iterable {
  private val store = Leveldb()
  private val namespace = "metric"
  type T = String

  def get[T](key: String, clazz: Class[T]): Future[Option[T]] = store.get(s"$namespace.$key", clazz)
  def put(key: String, value: AnyRef): Future[Ack] = store.put(s"$namespace.$key", value.toString)

  def isReady:Future[Boolean] = store.isReady
  def delete(key: String) = store.delete(s"$namespace.$key")
  def close(): Unit = store.close
}

class LastMessageKvs(container: String)(implicit val system: ActorSystem) extends Kvs with Iterable {
  private val store = Leveldb()
  private val namespace = "message"
  type T = String

  def get[T](key: String, clazz: Class[T]): Future[Option[T]] = store.get(s"$namespace.$key", clazz)
  def put(key: String, value: AnyRef): Future[Ack] = store.put(s"$namespace.$key", value.toString)

  def isReady:Future[Boolean] = store.isReady
  def delete(key: String) = store.delete(s"$namespace.$key")
  def close(): Unit = store.close
}
