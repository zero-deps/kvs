package zd.kvs
package rng

import akka.actor._
import com.typesafe.config.Config
import java.security.MessageDigest
import scala.annotation.tailrec
import scala.collection.SortedMap
import zd.proto.Bytes

class HashingImpl(config: Config) extends Extension {
  val hashLen = config.getInt("hash-length")
  val bucketsNum = config.getInt("buckets")
  val bucketRange = (math.pow(2, hashLen.toDouble) / bucketsNum).ceil.toInt

  def hash(word: Bytes): Int = {
    implicit val digester = MessageDigest.getInstance("MD5")
    digester update word.unsafeArray
    val digest = digester.digest

    (0 to hashLen / 8 - 1).foldLeft(0)((acc, i) =>
      acc | ((digest(i) & 0xff) << (8 * (hashLen / 8 - 1 - i)))
    ) //take first 4 byte
  }

  def findBucket(key: Bytes): Bucket = (hash(key) / bucketRange).abs

  def findNodes(hashKey: Int, vNodes: SortedMap[Bucket, Address], nodesNumber: Int): PreferenceList = {
    @tailrec
    def findBucketNodes(hashK: Int, nodes: PreferenceList): PreferenceList = {
      val it = vNodes.keysIteratorFrom(hashK)
      val hashedNode = if (it.hasNext) it.next() else vNodes.firstKey
      val node = vNodes(hashedNode)
      val prefList = nodes + node
      prefList.size match {
        case `nodesNumber` => prefList
        case _ => findBucketNodes(hashedNode + 1, prefList)
      }
    }

    findBucketNodes(hashKey, Set.empty[Node])
  }
}

object HashingExtension extends ExtensionId[HashingImpl] with ExtensionIdProvider {

  override def createExtension(system: ExtendedActorSystem): HashingImpl =
    new HashingImpl(system.settings.config.getConfig("ring"))

  override def lookup = HashingExtension
}


