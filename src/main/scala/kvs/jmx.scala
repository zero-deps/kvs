package mws.kvs

import java.lang.management.ManagementFactory
import javax.management.{InstanceAlreadyExistsException, InstanceNotFoundException, ObjectName, StandardMBean}

import akka.event.LoggingAdapter
import akka.util.{ByteString, Timeout}
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

import handle._,store._

/**
 * Kvs management access.
 */
trait KvsMBean {
  def all(feed:String):String
}
class KvsJmx(kvs:Kvs, log: LoggingAdapter) {
  private val server = ManagementFactory.getPlatformMBeanServer
  private val name = new ObjectName("akka:type=Kvs")
  implicit val timeout = Timeout(3 seconds)

  def createMBean() = {
    val mbean = new StandardMBean(classOf[KvsMBean]) with KvsMBean {

      def all(feed:String):String = {
        log.info(s"kvs.all($feed)")
        kvs.entries[En[String]](feed,None,None) match {
          case Right(list) => list.mkString(",\n")
          case Left(dbe) => dbe.msg
        }
      }
    }

    try {
      server.registerMBean(mbean, name)
      log.info("Registered KVS JMX MBean [{}]", name)
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
