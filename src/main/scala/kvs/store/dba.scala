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
  def close():Unit
  def isReady:Future[Boolean]
}
