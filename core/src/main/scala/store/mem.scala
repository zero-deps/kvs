package kvs
package store

import concurrent.Future

import java.util.concurrent.ConcurrentHashMap
import zero.ext._, option._, either._
import zd.proto.Bytes

object Mem {
  def apply(): Mem = new Mem
}

class Mem extends Dba {
  private val db = new ConcurrentHashMap[Key, Bytes]

  override def get(key: Key): Res[Option[Bytes]] = fromNullable(db.get(key)).right
  override def put(key: Key, value: Bytes): Res[Unit] = db.put(key, value).right.void
  override def delete(key: Key): Res[Unit] = db.remove(key).right.void

  override def load(path: String): Res[Any] = ???
  override def save(path: String): Res[String] = ???

  override def onReady(): Future[Unit] = Future.successful(())
  override def compact(): Unit = ()
}