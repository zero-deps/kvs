package mws.kvs
package handle

import store._

trait FdHandler extends Pickler[Fd] {
  import scala.pickling._,Defaults._,binary._,static._
  def pickle(e:Fd):Array[Byte] = e.pickle.value
  def unpickle(a:Array[Byte]):Fd = a.unpickle[Fd]

  def put(el:Fd)(implicit dba:Dba):Either[Err,Fd] = dba.put(el.id,pickle(el)).right.map(unpickle)
  def get(el:Fd)(implicit dba:Dba):Either[Err,Fd] = dba.get(el.id).right.map(unpickle)
  def delete(el:Fd)(implicit dba:Dba):Either[Err,Fd] = dba.delete(el.id).right.map(unpickle)
}
