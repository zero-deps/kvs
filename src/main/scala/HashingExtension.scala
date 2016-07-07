package mws.rng

import java.security.MessageDigest
import akka.actor._
import com.typesafe.config.Config

import scala.annotation.tailrec
import scala.collection.SortedMap

class HashingImpl(config: Config) extends  Extension {
  val hashLen = config.getInt("hashLength")
  val bucketsNum = config.getInt("buckets")
  val bucketRange = (math.pow(2, hashLen) / bucketsNum).ceil.toInt

  def hash(word: String): Int = {
    implicit val digester = MessageDigest.getInstance("MD5")
    digester update word.getBytes("UTF-8")
    val digest = digester.digest

    (0 to hashLen / 8 - 1).foldLeft(0)((acc, i) => acc | ((digest(i) & 0xff) << (8 * (hashLen / 8 - 1 - i)))) //take first 4 byte
  }

  def findBucket(key: Key): Bucket = (hash(key) / bucketRange).abs


  def findNodes(hashKey: Int, vNodes: SortedMap[Bucket, Address], nodesNumber: Int) = {
    @tailrec
    def findBucketNodes(hashK: Int, maxSearch: Int, nodes: PreferenceList): PreferenceList =
    maxSearch match {
      case 0 => nodes
      case _ =>
        val it = vNodes.keysIteratorFrom(hashKey)
        val hashedNode = if (it.hasNext) it.next() else vNodes.firstKey
        val node = vNodes(hashedNode)
        val prefList = if (nodes.contains(node)) nodes else nodes + node
        prefList.size match {
          case `nodesNumber` => prefList
          case _ => findBucketNodes(hashK + 1, maxSearch - 1,  prefList)
        }
    }
    findBucketNodes(hashKey, vNodes.size, Set.empty[Node])
  }
}

object HashingExtension extends ExtensionId[HashingImpl] with ExtensionIdProvider {

  override def createExtension(system: ExtendedActorSystem): HashingImpl =
    new HashingImpl(system.settings.config.getConfig("ring"))

  override def lookup = HashingExtension
}


