package zd.kvs
package store

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.Future
import zero.ext._, option._, either._
import zd.proto.Bytes

class Mem extends Dba {
  private val db = new ConcurrentHashMap[Bytes, Bytes]

  override def get(key: Bytes): Res[Option[Bytes]] = fromNullable(db.get(key)).right
  override def put(key: Bytes, value: Bytes): Res[Unit] = db.put(key, value).right.void
  override def delete(key: Bytes): Res[Unit] = db.remove(key).right.void

  override def load(path: String): Res[Any] = ???
  override def save(path: String): Res[String] = ???

  override def onReady(): Future[Unit] = Future.successful(())
  override def compact(): Unit = ()
}