package mws.kvs
package handle

import store._

trait ElHandler[T] extends Pickler[T] {
  def put(k:String,el:T)(implicit dba:Dba):Either[Err,T] = dba.put(k,pickle(el)).right.map(_=>el)
  def get(k:String)(implicit dba:Dba):Either[Err,T] = dba.get(k).right.map(unpickle)
  def delete(k:String)(implicit dba:Dba):Either[Err,T] = dba.delete(k).right.map(unpickle)
}
