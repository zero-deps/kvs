package mws.kvs

import java.lang.management.ManagementFactory
import javax.management.{ObjectName,StandardMBean}
import scala.util._
import akka.actor.ActorSystem

/** Kvs management access */
trait KvsMBean {
  def save():String
  def load(path:String):Any
}

class KvsJmx(kvs:Kvs,system:ActorSystem) {
  private val server = ManagementFactory.getPlatformMBeanServer
  private val name = new ObjectName("akka:type=Kvs")
  import system.log

  def createMBean():Unit = {
    val mbean = new StandardMBean(classOf[KvsMBean]) with KvsMBean {
      def save():String         = kvs.save().    getOrElse("timeout")
      def load(path:String):Any = kvs.load(path).getOrElse("timeout")
    }
    Try(server.registerMBean(mbean,name))
    log.info("Registered KVS JMX MBean [{}]",name)
  }

  def unregisterMBean():Unit = Try(server.unregisterMBean(name))
}
