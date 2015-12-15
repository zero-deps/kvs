package mws.kvs

/**
 * Container/Iterator types
 *
 * Case classed as functions and pickled take much less space when scala types.
 */
package object handle {
  case class Fd(id:String,top:Option[String]=None,count:Int=0)
  case class En[T](fid:String,id:String,prev:Option[String]=None,next:Option[String]=None,data:T)
}
