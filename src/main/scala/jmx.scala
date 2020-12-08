package kvs

import java.lang.management.ManagementFactory
import javax.management.{ObjectName,StandardMBean}
import scala.util._
import scala.annotation.unused

import store.Dba

/** Kvs management access */
trait KvsMBean {
  // def save(path: String): String
  // def load(path: String): Any
  // def compact(): String
}

class KvsJmx(@unused dba: Dba) {
  private val server = ManagementFactory.getPlatformMBeanServer
  private val name = new ObjectName("kvs:type=Kvs")

  def createMBean(): Unit = {
    val mbean = new StandardMBean(classOf[KvsMBean]) with KvsMBean {
      // def save(path: String): String = dba.save(path).fold(_.toString, identity)
      // def load(path: String): Any = dba.load(path).fold(_.toString, identity)

      // def compact(): String = {
      //   val t = System.nanoTime
      //   dba.compact()
      //   s"done in ${(System.nanoTime - t) / 1000000} ms"
      // }
    }
    Try(server.registerMBean(mbean,name))
    ()
  }

  def unregisterMBean(): Unit = {
    val _ = Try(server.unregisterMBean(name))
  }
}
