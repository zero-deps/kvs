package zd.kvs

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future
import util.chaining.*

class Mem extends Dba, AutoCloseable:
  private val db = TrieMap[String, V]()
  private val nextids = TrieMap[String, Long]()

  override def get(key: String): R[Option[V]] =
    db.get(key).pipe(Right.apply)
  
  override def put(key: String, value: V): R[Unit] =
    db.put(key, value).pipe(Right.apply).map(_ => unit)
  
  override def delete(key: String): R[Unit] =
    db.remove(key).pipe(Right.apply).map(_ => unit)

  override def nextid(fid: String): R[String] =
    Right(nextids.updateWith(fid){
      case None => Some(1)
      case Some(n) => Some(n+1)
    }.get.toString)

  override def onReady(): Future[Unit] = Future.successful(())
  override def compact(): Unit = ()

  override def load(path: String): R[String] = ???
  override def save(path: String): R[String] = ???
  override def deleteByKeyPrefix(k: K): R[Unit] = ???

  override def close(): Unit = unit
