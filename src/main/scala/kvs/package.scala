package mws

package object kvs{
  case class Dbe(name:String="error", msg:String)
  type Th   = Throwable
  type Err  = Dbe
}
