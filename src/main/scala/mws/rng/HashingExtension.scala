package mws.rng

import java.security.MessageDigest
import com.typesafe.config.Config

class HashingFunction(config: Config) {
  val hashLen = config.getInt("hashLength")
  val bucketsNum = config.getInt("buckets")
  val bucketRange = (math.pow(2, hashLen) / bucketsNum).ceil.toInt

  def hash(str:String):Int = {
    implicit val digester = MessageDigest.getInstance("MD5")
    digester update str.getBytes("UTF-8")
    val digest = digester.digest

    (0 to hashLen/8-1).foldLeft(0)((acc,i) => acc | ((digest(i) & 0xff)  << (8*(hashLen/8-1-i)))) //take first 4 byte
  }

  def findBucket(keyOrBucket:Either[Key,Bucket]):Bucket = keyOrBucket match {
    case Left(key:Key) => (hash(key) / bucketRange ).abs
    case Right(b) => b % bucketsNum
  }
}


