package mws.kvs

package object handle {

  type D  = Tuple2[String,String]

  type V    = String //  type V <: AnyVal
  type Ns   = String
  type Id   = String
  type Fid  = String
  type Fd   = Tuple3[Id,Option[String],Int]
  type FdId = Tuple2[Ns,Id]
  type En   = Tuple5[Fid,Id,Option[String],Option[String],V]

  case class Message(name:String="message", key:String, data:String, prev:Option[String]=None, next:Option[String]=None)
  case class Metric (name:String= "metric", key:String, data:String, prev:Option[String]=None, next:Option[String]=None)

  case class Feed(ns:String,id:String)
  case class User(cn:String, key:String, prev:Option[String]=None, next:Option[String]=None, data:String)
}