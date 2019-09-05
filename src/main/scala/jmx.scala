package zd.kvs

import akka.actor.ActorSystem
import java.lang.management.ManagementFactory
import javax.management.{ObjectName,StandardMBean}
import scala.util._
import zd.gs.z._

/** Kvs management access */
trait KvsMBean {
  def save(path: String): String
  def load(path: String): Any
  def get(k: String): String
  def put(k: String, v: String): Unit
  def putN(i: Int): Unit
  def getN(i: Int): Unit
  def compact(): String
}

class KvsJmx(kvs: Kvs, system: ActorSystem) {
  private val server = ManagementFactory.getPlatformMBeanServer
  private val name = new ObjectName("zd:type=Kvs")
  import system.log

  def createMBean(): Unit = {
    val mbean = new StandardMBean(classOf[KvsMBean]) with KvsMBean {
      def save(path: String): String = kvs.dump.save(path).fold(_.toString, identity)
      def load(path: String): Any = kvs.dump.load(path).fold(_.toString, identity)

      def get(k: String): String = kvs.el.get(k).fold(_.toString, _.cata(xs => new String(xs.toArray), "NaN"))
      def put(k: String, v: String): Unit = {
        val _ = kvs.el.put(k, v.getBytes)
      }

      def getN(i: Int): Unit = {
        println(s"started check")
        val s = System.nanoTime()
        ((i - 1) * 200000 to i * 200000).foreach(i =>
          if (get(s"ASD!@# $i") == s"KLKLKKL $i") {}
          else print(s"ne $i")
        )
        println(s"End check in ${(System.nanoTime() - s) / 1000000d} ms")
        println(s"One read in ${(System.nanoTime() - s) / 200000d} ns")
      }
      def putN(i: Int): Unit = {
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

      def compact(): String = {
        val t = System.nanoTime
        kvs.compact()
        s"done in ${(System.nanoTime - t) / 1000000} ms"
      }
    }
    Try(server.registerMBean(mbean,name))
    log.info("Registered KVS JMX MBean [{}]",name)
  }

  def unregisterMBean(): Unit = {
    val _ = Try(server.unregisterMBean(name))
  }
}
