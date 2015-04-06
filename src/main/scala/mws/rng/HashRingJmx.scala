package mws.rng

import java.lang.management.ManagementFactory
import javax.management.{InstanceAlreadyExistsException, InstanceNotFoundException, ObjectName, StandardMBean}

import akka.event.LoggingAdapter
import akka.util.Timeout

import scala.concurrent.duration._

/** JMX cient */
trait HashRingMBean {
  def get(key:String):String
  def put(key:String, data:String):String
  def delete(key:String):Unit
}

private[mws] class HashRingJmx(ring:HashRing, log: LoggingAdapter) {
  private val server = ManagementFactory.getPlatformMBeanServer
  private val name = new ObjectName("akka:type=Store")
  implicit val timeout = Timeout(2 seconds)

  def createMBean() = {
    val mbean = new StandardMBean(classOf[HashRingMBean]) with HashRingMBean {

      def get(key: String) = {
        val value = ring.get(key).map(_(0)._7).mkString
        log.info(s"val for $key -> $value")
        value
      }
      def put(key:String, value:String):String = ring.put(key, value)
      def delete(key:String) = ring.delete(key)
    }

    try {
      server.registerMBean(mbean, name)
      log.info("Registered hash ring JMX MBean [{}]", name)
    } catch {
      case e: InstanceAlreadyExistsException =>
    }
  }

  def unregisterMBean(): Unit = {
    try {
      server.unregisterMBean(name)
    } catch {
      case e: InstanceNotFoundException =>
    }
  }
}
