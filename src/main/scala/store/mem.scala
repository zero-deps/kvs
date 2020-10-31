package zd.kvs
package store

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.Future
import zero.ext._, option._, either._
import zd.proto.Bytes

class Mem extends Dba {
  private val db = new ConcurrentHashMap[String, Array[Byte]]
  private val nextids = new ConcurrentHashMap[String, java.lang.Long]

  def get(key: String): Res[Option[Array[Byte]]] = fromNullable(db.get(key)).right
  def put(key: String, value: Array[Byte]): Res[Array[Byte]] = { db.put(key, value); value }.right
  def delete(key: String): Res[Unit] = db.remove(key).right.void

  def nextid(fid: String): Res[String] = nextids.compute(fid, (_, v) => if (v == null) 1 else v + 1).right.map(_.toString)
  def onReady(): Future[Unit] = Future.successful(())
  def compact(): Unit = ()

  def load(path: String): Res[Any] = ???
  def save(path: String): Res[String] = ???
  def clean(keyPrefix: Bytes): Res[Unit] = ???
}
