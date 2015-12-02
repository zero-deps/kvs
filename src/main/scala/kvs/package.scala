package mws

package object kvs{
  case class Dbe(name:String="error", msg:String)

  type Th   = Throwable
  type D    = Tuple2[String,String]
  type Err  = Dbe

//  case class Entry(data: String, prev: Option[String], next: Option[String])
  case class Message(name:String="message", key:String, data:String, prev:Option[String]=None, next:Option[String]=None)
  case class Metric (name:String= "metric", key:String, data:String)

//  type StatMessage = Message // wtf
//  type StatMetric  = Metric  // wtf

  trait Data {
    def key: String
    def data:String
  }
}
