package zd.kvs
package store

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.Future
import proto.Bytes

class Mem extends Dba {
  private val db = new ConcurrentHashMap[String, Array[Byte]]
  private val nextids = new ConcurrentHashMap[String, java.lang.Long]

  def get(key: String): Res[Option[Array[Byte]]] = Right(Option(db.get(key)))
  def put(key: String, value: Array[Byte]): Res[Array[Byte]] = { db.put(key, value); Right(value) }
  def delete(key: String): Res[Unit] = { db.remove(key); Right(()) }

  def nextid(fid: String): Res[String] = Right(nextids.compute(fid, (_, v) => if (v == null) 1 else v + 1).toString)
  def onReady(): Future[Unit] = Future.successful(())
  def compact(): Unit = ()

  def load(path: String): Res[String] = ???
  def save(path: String): Res[String] = ???
  def clean(keyPrefix: Bytes): Res[Unit] = ???
}
