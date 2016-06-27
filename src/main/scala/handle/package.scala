package mws.kvs

/**
 * Container/Iterator types
 */
package object handle {
  val empty = "empty_8fc62083-b0d1-49cc-899c-fbb9ab177241"

  final case class Fd(id:String,top:String)
  final case class En[T](fid:String,id:String,prev:String,next:String,data:T)

  object Fd {
    def apply(id:String):Fd = Fd(id,top=empty)
  }

  object En {
    def apply[T](fid:String,id:String,data:T):En[T] = En(fid,id,prev=empty,next=empty,data)
    def apply[T](fid:String,id:String):En[T] = En(fid,id,prev=empty,next=empty,data=null.asInstanceOf[T])
  }
}
