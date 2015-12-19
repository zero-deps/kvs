package mws.kvs
package store

import scala.concurrent.Future
import scala.collection.concurrent.TrieMap
import com.typesafe.config.Config
import Memory.not_found

object Memory {
  val not_found:Err = Dbe(msg="not_found")

  def apply(cfg:Config): Dba = new Memory(cfg)
}
class Memory(cfg:Config) extends Dba {
  val storage = TrieMap[String, Array[Byte]]()

  def put(key:String,value:Array[Byte]):Either[Err,Array[Byte]] = {
    storage.put(key, value)
    Right(value)
  }
  def get(key:String):Either[Err,Array[Byte]] = storage.get(key) match {
    case Some(value) => Right(value)
    case None => Left(not_found)
  }
  def delete(key:String):Either[Err,Array[Byte]] = get(key).right.map {
    value => storage.remove(key); value
  }
  def close():Unit = ()
  def isReady:Future[Boolean] = Future.successful(true)
}