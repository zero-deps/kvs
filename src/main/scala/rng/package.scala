package zd.kvs

import akka.actor.{Address, ActorRef}

package object rng {
  type Bucket = Int
  type VNode = Int
  type Node = Address
  type Key = Array[Byte]
  type Value = Array[Byte]
  type VectorClock = akka.cluster.VectorClock
  type Age = (VectorClock, Long)
  type PreferenceList = Set[Node]

  val emptyVC = akka.cluster.emptyVC

  def stob(s: String): Array[Byte] = s.getBytes("UTF-8")
  def itob(v: Int): Array[Byte] = Array[Byte]((v >> 24).toByte, (v >> 16).toByte, (v >> 8).toByte, v.toByte)

  implicit class StringExt(value: String) {
    def blue: String = s"\u001B[34m${value}\u001B[0m"
    def green: String = s"\u001B[32m${value}\u001B[0m"
  }

  def now_ms(): Long = System.currentTimeMillis
  
  def addr(s: ActorRef): Node = s.path.address
}
