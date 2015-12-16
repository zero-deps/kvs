package mws.rng

import java.io.File

import akka.actor._
import akka.event.Logging
import akka.pattern.ask
import akka.routing.FromConfig
import akka.util.Timeout
import org.iq80.leveldb._
import scala.concurrent.Future
import scala.concurrent.duration._

object HashRing extends ExtensionId[HashRing] with ExtensionIdProvider{
  
  override def lookup = HashRing
  
  override def createExtension(system: ExtendedActorSystem): HashRing = {
    new HashRing(system)
  }
  override def get(system: ActorSystem):HashRing  = super.get(system)
}

class HashRing(val system:ExtendedActorSystem) extends Extension {
  implicit val timeout = Timeout(5.second)
  lazy val log = Logging(system, "hash-ring")
  lazy val clusterConfig = system.settings.config.getConfig("akka.cluster")
  system.eventStream
  var jmx: Option[HashRingJmx] = None
  //ADD connection
  val configPath = "ring.leveldb"
  val config = system.settings.config.getConfig(configPath)
  val nativeLeveldb = config.getBoolean("native")

  val leveldbOptions = new Options().createIfMissing(true)
  def leveldbReadOptions = new ReadOptions().verifyChecksums(config.getBoolean("checksum"))
  val leveldbWriteOptions = new WriteOptions().sync(config.getBoolean("fsync")).snapshot(false)
  val leveldbDir = new File(config.getString("dir"))
  
  val rngConf = system.settings.config.getConfig("ring")
  val bucketsNum = rngConf.getInt("buckets")
  var leveldb = leveldbFactory.open(leveldbDir, if (nativeLeveldb) leveldbOptions else leveldbOptions.compressionType(CompressionType.NONE))

  def leveldbFactory =
    if (nativeLeveldb) org.fusesource.leveldbjni.JniDBFactory.factory
    else org.iq80.leveldb.impl.Iq80DBFactory.factory

  // todo: create system/hashring superviser
  private val store= system.actorOf(Props(classOf[WriteStore],leveldb).withDeploy(Deploy.local), name="ring_write_store")
  private val gather = system.actorOf(Props[GathererDel].withDeploy(Deploy.local), name="ring_gatherer")
  private val readStore = system.actorOf(
    FromConfig.props(Props(classOf[ReadonlyStore], leveldb)).withDeploy(Deploy.local), name = "ring_readonly_store")
  private val hash = system.actorOf(Props(classOf[Hash], store, readStore).withDeploy(Deploy.local), name = "ring_hash")

  
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
    (hash ? Get(key)).mapTo[Option[Value]]
  }

  def put(k: String, v: Value): Future[Ack] = {
    //TODO create timestamp here from cluster clock
    (hash ? Put(k, v)).mapTo[Ack]
  }

  def delete(k: String): Future[Ack] = {
    (hash ? Delete(k)).mapTo[Ack]
  }

  def travers(fid:String, start: Option[Int], end: Option[Int]):Future[List[Value]] = {
    (hash ? Traverse(fid, start, end)).mapTo[List[Value]]
  }

  def add(fid:String, v: Value) = {
    (hash ? Add(fid,v)).mapTo[Int]
  }
  
  def isReady: Future[Boolean] = (hash ? Ready).mapTo[Boolean]
}