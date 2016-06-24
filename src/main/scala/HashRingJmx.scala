package mws.rng

import java.lang.management.ManagementFactory
import javax.management.{InstanceAlreadyExistsException, InstanceNotFoundException, ObjectName, StandardMBean}
import scala.language.postfixOps
import scala.concurrent.Await
import scala.concurrent.duration._
import akka.event.LoggingAdapter
import akka.util.{ByteString, Timeout}

/** JMX cient */
trait HashRingMBean {
  def get(key:String): String
  def put(key:String, data: String):String
  def delete(key:String):Unit
  def add(fid: String, v: String) : Int
  def travers(fid: String, start: Int, end: Int): List[Value]
}

private[mws] class HashRingJmx(ring:HashRing, log: LoggingAdapter) {
  private val server = ManagementFactory.getPlatformMBeanServer
  private val name = new ObjectName("akka:type=Store")
  implicit val timeout = Timeout(3 seconds)

  def createMBean() = {
    val mbean = new StandardMBean(classOf[HashRingMBean]) with HashRingMBean {

      def get(key: String) = {
        val value = Await.result(ring.get(key), timeout.duration)
        log.info(s"val for $key -> $value")
        value match {
          case Some(byteStr) => new String(byteStr.toArray)
          case None => "not_present"
        }
      }

      def put(key: String, value: String): String = {
        Await.result(ring.put(key, ByteString(value)),timeout.duration) match {
          case AckSuccess => "ok"
          case AckQuorumFailed => "quorum failed"
          case AckTimeoutFailed => "failed by timeout"
        }
      }
      def delete(key:String) = ring.delete(key)

      override def travers(fid: String, start:Int, end: Int): List[Value] = {
        Await.result(ring.travers(fid,
          if(start > 0)Some(start) else None,
        if(end > 0) Some(end) else None),timeout.duration) match {
          case list => list
        }
      }

      def add(fid: String, v: String): Int = {
        Await.result(ring.add(fid, ByteString(v)), timeout.duration)
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
