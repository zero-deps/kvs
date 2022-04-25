package kvs.rng

import akka.actor.{Address, ActorRef}
import proto.*

type Bucket = Int
type VNode = Int
type Node = Address
type Key = Array[Byte]
type Value = Array[Byte]
type VectorClock = akka.cluster.VectorClock
type Age = (VectorClock, Long)
type PreferenceList = Set[Node]

val emptyVC = akka.cluster.emptyVC

extension (value: String)
  def blue: String = s"\u001B[34m${value}\u001B[0m"
  def green: String = s"\u001B[32m${value}\u001B[0m"

def now_ms(): Long = System.currentTimeMillis

def addr(s: ActorRef): Node = s.path.address

given MessageCodec[PortVNode] = caseCodecAuto
