package kvs

import akka.actor.{Address, ActorRef}
import zd.proto.api.MessageCodec
import zd.proto.macrosapi.caseCodecAuto

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

  implicit class StringExt(value: String) {
    def blue: String = s"\u001B[34m${value}\u001B[0m"
    def green: String = s"\u001B[32m${value}\u001B[0m"
  }

  def now_ms(): Long = System.currentTimeMillis
  
  def addr(s: ActorRef): Node = s.path.address

  implicit val PortVNodeC: MessageCodec[PortVNode] = caseCodecAuto[PortVNode]

  type RngConf = kvs.store.Rng.Conf
  type LvlConf = kvs.store.Rng.LvlConf
}
