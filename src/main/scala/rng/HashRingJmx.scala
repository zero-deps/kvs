package mws.rng

import java.lang.management.ManagementFactory
import javax.management.{InstanceAlreadyExistsException, InstanceNotFoundException, ObjectName, StandardMBean}

import scalaz._

import akka.event.LoggingAdapter

import mws.kvs.store.Ring

/** JMX cient */
trait HashRingMBean {
  def get(key:String): String
  def put(key:String, data: String):String
  def delete(key:String):String
}

private[mws] class HashRingJmx(ring:Ring, log: LoggingAdapter) {
  private val server = ManagementFactory.getPlatformMBeanServer
  private val name = new ObjectName("akka:type=Store")

  def createMBean() = {
    val mbean = new StandardMBean(classOf[HashRingMBean]) with HashRingMBean {

      def get(key: String): String = ring.get(key) match {
        case \/-(byteStr) => new String(byteStr.toArray)
        case -\/(_) => "not_present"
      }

      def put(key: String, value: String): String = ring.put(key, value.getBytes) match {
          case \/-(_) => "ok"
          case -\/(e) => s"$e"
      }

      def delete(key:String) = ring.delete(key)match {
          case \/-(_) => "ok"
          case -\/(e) => s"error: $e"
      }
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
