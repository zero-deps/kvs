package zd.kvs
package store

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future
import zero.ext.*, option.*

class Mem extends Dba {
  private val db = new TrieMap[String, Array[Byte]]
  private val nextids = new TrieMap[String, Long]

  def get(key: String): Res[Option[Array[Byte]]] = db.get(key).right
  def put(key: String, value: Array[Byte]): Res[Array[Byte]] = { db.put(key, value); value }.right
  def delete(key: String): Res[Unit] = { db.remove(key); Right(()) }

  def nextid(fid: String): Res[String] =
    nextids.updateWith(fid){
      case None => 1L.some
      case Some(n) => (n + 1).some
    }.get.toString.right

  def onReady(): Future[Unit] = Future.successful(())
  def compact(): Unit = ()

  def load(path: String): Res[String] = ???
  def save(path: String): Res[String] = ???
  def clean(keyPrefix: Array[Byte]): Res[Unit] = ???
}
