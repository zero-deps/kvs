package mws.rng

import akka.actor._
import akka.kernel.Bootable
import akka.util.ByteString
import com.typesafe.config.ConfigFactory

/** Ring application API.*/
trait RingAPI{
  def get(key:String):ByteString
  def put(key:String, data:ByteString):Unit
  def delete(key:String):Unit
}

class RingApp extends Bootable {
  implicit val system = ActorSystem("rng", ConfigFactory.load)

  override def startup(): Unit = HashRing(system)
  override def shutdown():Unit = system.shutdown
}
