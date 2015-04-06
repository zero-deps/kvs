package mws.rng

import java.security.MessageDigest

import akka.actor._
import akka.event.Logging
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.Await
import scala.concurrent.duration._

/**
 * Created by doxtop on 12.02.15.
 */
object HashRing extends ExtensionId[HashRing] with ExtensionIdProvider{
  override def lookup = HashRing
  override def createExtension(system: ExtendedActorSystem): HashRing = new HashRing(system)
  override def get(system: ActorSystem):HashRing  = super.get(system)
}

class HashRing(val system:ExtendedActorSystem) extends Extension {
  //import RingEvent._
  implicit val timeout = Timeout(1 second)
  implicit val digester = MessageDigest.getInstance("MD5")
  lazy val config = system.settings.config.getConfig("ring")
  lazy val log = Logging(system, "hash-ring")
  lazy val clusterConfig = system.settings.config.getConfig("akka.cluster")

  var jmx: Option[HashRingJmx] = None

  // todo: create system/hashring superviser
  private val hash = system.actorOf(Props[Hash].withDeploy(Deploy.local), name="ring_hash")
  private val store= system.actorOf(Props[Store].withDeploy(Deploy.local), name="ring_store")

  if (clusterConfig.getBoolean("jmx.enabled")) jmx = {
    val jmx = new HashRingJmx(this, log)
    jmx.createMBean()
    Some(jmx)
  }

  system.registerOnTermination(shutdown())
  
  private[mws] def shutdown():Unit= {
    jmx foreach {_.unregisterMBean}
    log.info("Hash ring down")
  }
  
  // internal
  
  
  
  
  
  // Public API

  def get(key:String):Option[List[Data]] = {
    log.info(s"get $key")
    Option(Await.result(hash ? Get(key), timeout.duration).asInstanceOf[List[Data]])
  }

  def put(k: String, v: String): String = {
    log.info(s"put $k -> $v")
    Await.result(hash ? Put(k, v), timeout.duration).asInstanceOf[String]
  }

  def delete(k: String): String = {
    log.info(s" delete $k")
    Await.result(hash ? Delete(k), timeout.duration).asInstanceOf[String]
  }
}


/*
  /* ===== Consistence hash routing example ===== */
  import akka.routing.ConsistentHashingRouter
  import akka.routing.ConsistentHashingRouter.ConsistentHashMapping

  case class Entry(key: String, value: String)

  class Cache extends Actor {
    var cache = Map.empty[String, String]

    def receive = {
      case Entry(key, value) => cache += (key -> value)
      case key: String       => sender ! cache.get(key)
    }
  }

  def hashMapping: ConsistentHashMapping = {
    case Entry(key, _) => key
    case s: String     => s
  }
  val cache = system.actorOf(Props[Cache].withRouter(
    ConsistentHashingRouter(10, hashMapping = hashMapping)),
    name = "cache")

  //cache ! Entry("hello", "HELLO")
  //cache ? "hello" onSuccess { case x => println(x) }

  /* ============================ */
*/
