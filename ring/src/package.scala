package kvs.rng

import org.apache.pekko.actor.{Address, ActorRef}
import proto.*

type Bucket = Int
type VNode = Int
type Node = Address
type Key = Array[Byte]
type Value = Array[Byte]
type VectorClock = org.apache.pekko.cluster.VectorClock
type Age = (VectorClock, Long)
type PreferenceList = Set[Node]

val emptyVC = org.apache.pekko.cluster.emptyVC

extension (value: String)
  def blue: String = s"\u001B[34m${value}\u001B[0m"
  def green: String = s"\u001B[32m${value}\u001B[0m"

def now_ms(): Long = System.currentTimeMillis

def addr(s: ActorRef): Node = s.path.address

given MessageCodec[PortVNode] = caseCodecAuto
