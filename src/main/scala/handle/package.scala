package mws.kvs

/**
 * Container/Iterator types
 */
package object handle {
  val empty = "empty_8fc62083-b0d1-49cc-899c-fbb9ab177241"

  final case class Fd(id:String,top:String=empty,count:Int=0)

  trait Entry {
    val fid: String
    val id: String
    val prev: String
  }
  final case class En[T](fid:String,id:String=empty,prev:String=empty,data:T) extends Entry
}
