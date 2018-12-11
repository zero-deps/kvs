package mws.kvs

import akka.actor.ActorSystem
import java.lang.management.ManagementFactory
import javax.management.{ObjectName,StandardMBean}
import mws.kvs.el.ElHandler.strHandler
import scala.util._

/** Kvs management access */
trait KvsMBean {
  def save(path: String): String
  def load(path: String): Any
  def get(k: String): String
  def put(k: String, v: String): Unit
  def load(i: Int): Unit
  def check(i: Int): Unit
  def compact(): String
}

class KvsJmx(kvs: Kvs, system: ActorSystem) {
  private val server = ManagementFactory.getPlatformMBeanServer
  private val name = new ObjectName("akka:type=Kvs")
  import system.log

  def createMBean(): Unit = {
    val mbean = new StandardMBean(classOf[KvsMBean]) with KvsMBean {
      override def save(path: String): String = kvs.dump.save(path).getOrElse("timeout")
      override def load(path: String): Any = kvs.dump.load(path).getOrElse("timeout")
      override def get(k: String): String = kvs.el.get(k).getOrElse("NaN")
      override def put(k: String, v: String): Unit = kvs.el.put(k, v)
      override def check(i: Int): Unit = {
        println(s"started check")
        val s = System.nanoTime()
        ((i - 1) * 200000 to i * 200000).foreach(i =>
          if (get(s"ASD!@# $i") == s"KLKLKKL $i") {}
          else print(s"ne $i")
        )
        println(s"End check in ${(System.nanoTime() - s) / 1000000d} ms")
        println(s"One read in ${(System.nanoTime() - s) / 200000d} ns")
      }
      override def load(i: Int): Unit = {
        println(s" Start load ")
        val s = System.nanoTime()
        var t = System.nanoTime()
        ((i - 1) * 200000 to i * 200000).foreach{ i =>
          if (i % 10000 == 0) {
            println(s"${i} in ${(System.nanoTime() - t) / 1000000d} ms")
            t = System.nanoTime()
          } else ""
          put(s"ASD!@# $i", s"KLKLKKL $i")
        }
        println(s"End load in ${(System.nanoTime() - s) / 1000000d} ms")
        println(s"One write in ${(System.nanoTime() - s) / 200000d} ns")
      }
      override def compact(): String = {
        val t = System.nanoTime
        kvs.compact()
        s"done in ${(System.nanoTime - t) / 100000} ms"
      }
    }
    Try(server.registerMBean(mbean,name))
    log.info("Registered KVS JMX MBean [{}]",name)
  }

  def unregisterMBean(): Unit = Try(server.unregisterMBean(name))
}
