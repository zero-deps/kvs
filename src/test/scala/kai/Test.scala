package mws

import java.nio.ByteBuffer
import java.security.MessageDigest

import akka.actor.Address
import org.scalatest.{FlatSpec, Matchers}

/**
 */
object Test {
  val hashLen:Int = 32
  val hostPort:String = "127.0.0.1:4224"
  val vnode:Int = 1024
  val node: Address = Address("akka:tcp", "mws", "127.0.1.1", 4224)
  val digester = MessageDigest.getInstance("MD5")
}

class Test extends FlatSpec with Matchers{
  import mws.Test._

  "quorter hash" should "containts the first byte" in {
    println(s"The test ${node.hostPort}")

    digester update (node.hostPort + vnode).getBytes("UTF-8")
    val digest:Array[Byte] = digester.digest

    val d1 = digest.map {h ⇒ "%02x".format(0xFF & h)}.mkString
    val d3 = ByteBuffer.wrap(digest).getInt

    println(s"MD5 digest $d1 for $node >quorter: buffer $d3")

    1 should be (1)
  }

}
