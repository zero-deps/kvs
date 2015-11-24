package mws

package object kvs{
  trait Data {
    def key: String
    def serialize: String
  }

  case class Entry(data: String, prev: Option[String], next: Option[String])
}
