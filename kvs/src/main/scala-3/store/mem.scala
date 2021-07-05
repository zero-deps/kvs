package zd.kvs

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future
import util.chaining.*

class Mem extends Dba, AutoCloseable:
  private val db = TrieMap[String, V]()

  override def get(key: String): R[Option[V]] =
    db.get(key).pipe(Right.apply)
  
  override def put(key: String, value: V): R[Unit] =
    db.put(key, value).pipe(Right.apply).map(_ => unit)
  
  override def delete(key: String): R[Unit] =
    db.remove(key).pipe(Right.apply).map(_ => unit)

  override def onReady(): Future[Unit] = Future.successful(())
  override def compact(): Unit = ()

  override def load(path: String): R[String] = ???
  override def save(path: String): R[String] = ???
  override def deleteByKeyPrefix(k: K): R[Unit] = ???

  override def close(): Unit = unit
