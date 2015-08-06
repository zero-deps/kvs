package mws.rng

import java.security.MessageDigest

import akka.actor.{ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import com.typesafe.config.Config

/**
 * Calculating bucket number by key.
 *
 */
class HashingImpl(config: Config) extends  Extension{
  val hashLen = config.getInt("hashLength")
  val bucketsNum = config.getInt("buckets")
  val bucketRange = (math.pow(2, hashLen) / bucketsNum).ceil.toInt


  /**
    * digest.take(4).zipWithIndex.foldLeft[Int](0)((out,x) => out | ((x._1 & 0xff) << 8*(3-x._2)))
   */
  def hash(keyOrBucket:Either[(Node,VNode), Key]):Int = {
    val msg:Array[Byte] = (keyOrBucket match {
      case Left((node:Node,vnode:VNode)) => node.hostPort + vnode
      case Right(key:Key) => key
    }).getBytes("UTF-8")

    implicit val digester = MessageDigest.getInstance("MD5") //TODO remove from field
    digester update msg
    val digest = digester.digest

    (0 to hashLen/8-1).foldLeft(0)((acc,i) => acc | ((digest(i) & 0xff)  << (8*(hashLen/8-1-i)))) //take first 4 byte
  }

  def findBucket(keyOrBucket:Either[Key,Bucket]):Bucket = keyOrBucket match {
    case Left(key:Key) => (hash(Right(key)) / bucketRange ).abs
    case Right(bucket) => bucket % bucketsNum
  }
}

object HashingExtension extends ExtensionId[HashingImpl] with ExtensionIdProvider {

  override def createExtension(system: ExtendedActorSystem): HashingImpl =
    new HashingImpl(system.settings.config.getConfig("ring"))

  override def lookup = HashingExtension
}


