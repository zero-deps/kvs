package zd.kvs

import java.lang.management.ManagementFactory
import javax.management.{ObjectName,StandardMBean}
import scala.util._

/** Kvs management access */
trait KvsMBean {
  def save(path: String): String
  def load(path: String): Any
  def compact(): String
}

class KvsJmx(kvs: Kvs) {
  private val server = ManagementFactory.getPlatformMBeanServer
  private val name = new ObjectName("kvs:type=Kvs")

  def createMBean(): Unit = {
    val mbean = new StandardMBean(classOf[KvsMBean]) with KvsMBean {
      def save(path: String): String = kvs.dump.save(path).fold(_.toString, identity)
      def load(path: String): Any = kvs.dump.load(path).fold(_.toString, identity)

      def compact(): String = {
        val t = System.nanoTime
        kvs.compact()
        s"done in ${(System.nanoTime - t) / 1000000} ms"
      }
    }
    Try(server.registerMBean(mbean,name))
    ()
  }

  def unregisterMBean(): Unit = {
    val _ = Try(server.unregisterMBean(name))
  }
}
