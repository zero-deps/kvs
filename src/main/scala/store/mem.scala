package kvs
package store

import concurrent.Future

import java.util.concurrent.ConcurrentHashMap
import zero.ext._, option._, either._
import zd.proto.Bytes

object Mem {
  def apply(): Mem = new Mem
}

class Mem extends Dba with AutoCloseable {
  private val db = new ConcurrentHashMap[Key, Bytes]

  def get(key: Key): Res[Option[Bytes]] = fromNullable(db.get(key)).right
  def put(key: Key, value: Bytes): Res[Unit] = db.put(key, value).right.void
  def delete(key: Key): Res[Unit] = db.remove(key).right.void

  def load(path: String): Res[Any] = ???
  def save(path: String): Res[String] = ???

  def onReady(): Future[Unit] = Future.successful(())
  def compact(): Unit = ()
  def close(): Unit = ()
}