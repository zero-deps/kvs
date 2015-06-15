package mws.rng

import java.security.MessageDigest

import akka.actor._
import akka.event.Logging
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

/**
 * Created by doxtop on 12.02.15.
 */
object HashRing extends ExtensionId[HashRing] with ExtensionIdProvider{
  override def lookup = HashRing
  override def createExtension(system: ExtendedActorSystem): HashRing = new HashRing(system)
  override def get(system: ActorSystem):HashRing  = super.get(system)
}

class HashRing(val system:ExtendedActorSystem) extends Extension {

  implicit val timeout = Timeout(1 second)
  implicit val digester = MessageDigest.getInstance("MD5")
  lazy val config = system.settings.config.getConfig("ring")
  lazy val log = Logging(system, "hash-ring")
  lazy val clusterConfig = system.settings.config.getConfig("akka.cluster")

  var jmx: Option[HashRingJmx] = None

  // todo: create system/hashring superviser
  private val hash = system.actorOf(Props[Hash].withDeploy(Deploy.local), name="ring_hash")
  private val store= system.actorOf(Props[Store].withDeploy(Deploy.local), name="ring_store")
  private val gather = system.actorOf(Props[Gatherer].withDeploy(Deploy.local), name="ring_gatherer")

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

  def get(key:String): Future[Option[Value]] = {
    log.info(s"get $key")
    (hash ? Get(key)).mapTo[Option[Value]]
  }

  def put(k: String, v: String): Future[Ack] = {
    log.info(s"put $k -> $v")
    //TODO create timestamp here
    (hash ? Put(k, v)).mapTo[Ack]
  }

  def delete(k: String): String = {
    log.info(s" delete $k")
    Await.result(hash ? Delete(k), timeout.duration).asInstanceOf[String]
  }
}