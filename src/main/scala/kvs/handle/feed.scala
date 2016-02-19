package mws.kvs
package handle

import store._
import scala.language.postfixOps

/**
 * Feed type handler.
 */
private trait hackPickling {
  import scala.pickling._,Defaults._,binary._
  def pickle(e:Fd) = e.pickle.value
  def unpickle(a:Array[Byte]):Fd = a.unpickle[Fd]
}
trait FdHandler extends Handler[Fd] {
  import scala.pickling._,Defaults._,binary._,static._
  def pickle(e:Fd) = e.pickle.value
  def unpickle(a:Array[Byte]):Fd = a.unpickle[Fd]

  def get(k: String)(implicit dba: Dba): Either[Err,Fd] =
    dba.get(k).right.map(unpickle)

  def put(el: Fd)(implicit dba: Dba): Either[Err,Fd] =
    dba.put(el.id, el.pickle.value).right.map {_ => el}

  def delete(k: String)(implicit dba: Dba): Either[Err,Fd] =
    get(k).right.map{ _=> dba.delete(k).right.map(unpickle)}.joinRight

  def entries(fid:String, from:Option[Fd], count:Option[Int])(implicit dba: Dba): Either[Err,List[Fd]] = ???

  def add(el: Fd)(implicit dba: Dba): Either[Err,Fd] = put(el)
  def remove(el: Fd)(implicit dba: Dba): Either[Err,Fd] = delete(el.id)
}
