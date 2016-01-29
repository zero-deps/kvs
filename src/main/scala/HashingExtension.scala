package mws.rng

import java.security.MessageDigest
import akka.actor.{ExtendedActorSystem, ExtensionIdProvider, ExtensionId, Extension}
import com.typesafe.config.Config

class HashingImpl(config: Config) extends  Extension{
  val hashLen = config.getInt("hashLength")
  val bucketsNum = config.getInt("buckets")
  val bucketRange = (math.pow(2, hashLen) / bucketsNum).ceil.toInt

  def hash(word: String):Int = {
    implicit val digester = MessageDigest.getInstance("MD5")
    digester update word.getBytes("UTF-8")
    val digest = digester.digest

    (0 to hashLen/8-1).foldLeft(0)((acc,i) => acc | ((digest(i) & 0xff)  << (8*(hashLen/8-1-i)))) //take first 4 byte
  }

  def findBucket(key: Key):Bucket = (hash(key) / bucketRange ).abs
}

object HashingExtension extends ExtensionId[HashingImpl] with ExtensionIdProvider {

  override def createExtension(system: ExtendedActorSystem): HashingImpl =
    new HashingImpl(system.settings.config.getConfig("ring"))

  override def lookup = HashingExtension
}


