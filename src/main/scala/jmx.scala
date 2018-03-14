package mws.kvs

import java.lang.management.ManagementFactory
import javax.management.{ObjectName,StandardMBean}
import scala.util._
import akka.actor.ActorSystem

/** Kvs management access */
trait KvsMBean {
  def save(path: String):String
  def load(path:String):Any
  def get(k: String): String
  def put(k: String, v: String): Unit
//  def load(i: Int): Unit
//  def check(i: Int): Unit
}

class KvsJmx(kvs:Kvs,system:ActorSystem) {
  private val server = ManagementFactory.getPlatformMBeanServer
  private val name = new ObjectName("akka:type=Kvs")
  import system.log

  def createMBean():Unit = {
    val mbean = new StandardMBean(classOf[KvsMBean]) with KvsMBean {
      def save(path: String):String         = kvs.save(path).    getOrElse("timeout")
      def load(path:String):Any = kvs.load(path).getOrElse("timeout")
      import mws.kvs.handle.ElHandler.strHandler
      def get(k: String): String = kvs.get(k).getOrElse("NaN")
      def put(k: String,v: String): Unit = kvs.put(k, v)
//      def check(i: Int): Unit = {
//        println(s"started check")
//        (0 to i * 500000).foreach(i =>{
//
//         if( get(s"ASD!@# $i") == s"KLKLKKL $i") {}
//         else {print(s"ne $i")}
//        })
//
//        println(s"finished check")
//
//      }

//      def load(i : Int) : Unit = {
//        println(s" Start load ")
//        val s = System.nanoTime()
//        (0 to i * 500000).foreach(i => {
//          if (i % 10000 == 0) { println(i) } else { ""}
//          put(s"ASD!@# $i", s"KLKLKKL $i")
//        }
//        )
//        println(s" End load in ${(System.nanoTime() - s ) / 1000000}   mls")
//      }

    }
    Try(server.registerMBean(mbean,name))
    log.info("Registered KVS JMX MBean [{}]",name)
  }

  def unregisterMBean():Unit = Try(server.unregisterMBean(name))
}
