package zd.kvs
package store

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future

class Mem extends Dba {
  private val db = new TrieMap[String, Array[Byte]]
  private val nextids = new TrieMap[String, Long]

  def get(key: String): Res[Option[Array[Byte]]] = Right(db.get(key))
  def put(key: String, value: Array[Byte]): Res[Array[Byte]] = { db.put(key, value); Right(value) }
  def delete(key: String): Res[Unit] = { db.remove(key); Right(()) }

  def nextid(fid: String): Res[String] =
    Right(nextids.updateWith(fid){
      case None => Some(1L)
      case Some(n) => Some(n + 1)
    }.get.toString)

  def onReady(): Future[Unit] = Future.successful(())
  def compact(): Unit = ()

  def load(path: String): Res[String] = ???
  def save(path: String): Res[String] = ???
  def clean(keyPrefix: Array[Byte]): Res[Unit] = ???
}
