package mws.kvs
package store

import scala.concurrent.Future

/**
 * Database Application Interface.
 */
trait Dba {
  type V = Array[Byte]
  def put(key:String,value:V):Either[Err,V]
  def get(key:String)        :Either[Err,V]
  def delete(key:String)     :Either[Err,V]
  def save(): Future[String]
  def load(path:String):Future[Any]
  def iterate(path:String,foreach:(String,Array[Byte])=>Unit):Future[Any]
  def close():Unit
  def isReady:Future[Boolean]
}
