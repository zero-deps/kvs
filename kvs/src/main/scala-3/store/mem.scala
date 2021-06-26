package zd.kvs

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future

class Mem extends Dba {
  private val db = new TrieMap[String, Array[Byte]]
  private val nextids = new TrieMap[String, Long]

  def get(key: String): Either[Err, Option[Array[Byte]]] = Right(db.get(key))
  def put(key: String, value: Array[Byte]): Either[Err, Array[Byte]] = { db.put(key, value); Right(value) }
  def delete(key: String): Either[Err, Unit] = { db.remove(key); Right(()) }

  def nextid(fid: String): Either[Err, String] =
    Right(nextids.updateWith(fid){
      case None => Some(1L)
      case Some(n) => Some(n + 1)
    }.get.toString)

  def onReady(): Future[Unit] = Future.successful(())
  def compact(): Unit = ()

  def load(path: String): Either[Err, String] = ???
  def save(path: String): Either[Err, String] = ???
  def deleteByKeyPrefix(keyPrefix: Array[Byte]): Either[Err, Unit] = ???
}
