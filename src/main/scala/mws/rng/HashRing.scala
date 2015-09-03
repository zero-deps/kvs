package mws.rng

import akka.actor._
import akka.event.Logging
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.Future
import scala.concurrent.duration._


/**
 * Created by doxtop on 12.02.15.
 */
object HashRing extends ExtensionId[HashRing] with ExtensionIdProvider{
  
  override def lookup = HashRing
  
  override def createExtension(system: ExtendedActorSystem): HashRing = {
    new HashRing(system)
  }
  override def get(system: ActorSystem):HashRing  = super.get(system)
}

class HashRing(val system:ExtendedActorSystem) extends Extension {
  implicit val timeout = Timeout(3.second)
  lazy val log = Logging(system, "hash-ring")
  lazy val clusterConfig = system.settings.config.getConfig("akka.cluster")
  system.eventStream
  var jmx: Option[HashRingJmx] = None

  // todo: create system/hashring superviser
  private val store= system.actorOf(Props[Store].withDeploy(Deploy.local), name="ring_store")
  private val hash = system.actorOf(Props(classOf[Hash], store).withDeploy(Deploy.local), name = "ring_hash")
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

  def put(k: String, v: Value): Future[Ack] = {
    log.info(s"put $k -> $v")
    //TODO create timestamp here
    (hash ? Put(k, v)).mapTo[Ack]
  }

  def delete(k: String): Future[Ack] = {
    log.info(s" delete $k")
    (hash ? Delete(k)).mapTo[Ack]
  }
  
  def isReady: Future[Boolean] = (hash ? Ready).mapTo[Boolean]
}