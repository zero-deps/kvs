package mws

package object kvs {
  case class Dbe(name:String="error", msg:String)
  type Err  = Dbe

  type Res[T] = Either[Err,T]
}
