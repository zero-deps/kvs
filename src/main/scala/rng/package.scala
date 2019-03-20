package mws

import akka.actor.{Address, ActorRef}
import akka.cluster.VectorClock
import com.google.protobuf.ByteString
import mws.rng.data.{Vec}
import scala.collection.breakOut
import scala.collection.immutable.{TreeMap}
import scalaz._

package object rng {
  type Bucket = Int
  type VNode = Int
  type Node = Address
  type Key = Array[Byte]
  type Value = Array[Byte]
  type VectorClockList = Vector[Vec]
  type Age = (VectorClock, Long)
  type PreferenceList = Set[Node]

  sealed trait Ack
  final case class AckSuccess(v: Option[Value]) extends Ack
  final case class AckQuorumFailed(why: String) extends Ack
  final case class AckTimeoutFailed(on: String) extends Ack

  sealed trait FsmState
  final case object ReadyCollect extends FsmState
  final case object Collecting extends FsmState
  final case object Sent extends FsmState
  
  def makevc(l: VectorClockList): VectorClock = new VectorClock(TreeMap.empty[String, Long] ++ l.map(a => a.key -> a.value))
  def fromvc(vc: VectorClock): VectorClockList = vc.versions.map((Vec.apply _).tupled)(breakOut)

  def stob(s: String): Array[Byte] = ByteString.copyFrom(s, "UTF-8").toByteArray
  def itob(v: Int): Array[Byte] = Array[Byte]((v >> 24).toByte, (v >> 16).toByte, (v >> 8).toByte, v.toByte)

  implicit class StringExt(value: String) {
    def blue: String = s"\u001B[34m${value}\u001B[0m"
    def green: String = s"\u001B[32m${value}\u001B[0m"
  }

  implicit val ByteStringEqual: Equal[Array[Byte]] = Equal.equalA

  def now_ms(): Long = System.currentTimeMillis
  
  def addr(s: ActorRef): Node = s.path.address
}
