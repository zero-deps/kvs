package mws.rng

import java.io.File
import akka.actor._
import akka.event.Logging
import akka.pattern.ask
import akka.routing.FromConfig
import akka.util.Timeout
import feed.{Traverse, Add, ChainCoordinator}
import org.iq80.leveldb._
import mws.rng.store.{WriteStore, ReadonlyStore}
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
  val config = system.settings.config.getConfig("ring.leveldb")
  val nativeLeveldb:Boolean = sys.props.get("os.name") match {
    case Some(os) if os.startsWith("Windows") =>
      log.info("Forcing usage of Java ported LevelDB for Windows OS"); false
    case _ => config.getBoolean("native")
  }

  val leveldbOptions = new Options().createIfMissing(true)
  val leveldbDir = new File(config.getString("dir"))
  var leveldb = leveldbFactory.open(leveldbDir, if (nativeLeveldb) leveldbOptions else leveldbOptions.compressionType(CompressionType.NONE))

  def leveldbFactory =
    if (nativeLeveldb) org.fusesource.leveldbjni.JniDBFactory.factory
    else org.iq80.leveldb.impl.Iq80DBFactory.factory

  system.actorOf(Props(classOf[WriteStore],leveldb).withDeploy(Deploy.local), name="ring_write_store")
  system.actorOf(FromConfig.props(Props(classOf[ReadonlyStore], leveldb)).withDeploy(Deploy.local), name = "ring_readonly_store")
  private val hash = system.actorOf(Props(classOf[Hash]).withDeploy(Deploy.local), name = "ring_hash")
  private val feedCoordinator = system.actorOf(Props(new ChainCoordinator(leveldb)), name = "coordinator")

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

  def dump(): Future[String] = (hash ? Dump).mapTo[String]

  def load(dumpPath: String): Future[Any] = hash ? LoadDump(dumpPath)

  def iterate(dumpPath: String, foreach: (String,Array[Byte])=>Unit): Future[Any] = hash ? IterateDump(dumpPath,foreach)

  def add(fid: String, v: Value) = {
    log.info(s"[api] add $fid , v = $v, send to $feedCoordinator")
    feedCoordinator ! fid
    feedCoordinator ? Add(fid,v)
  }

  def travers(fid: String, start: Option[String], count: Option[Int]): Future[Either[String,List[Value]]] =
    (feedCoordinator ? Traverse(fid, start,count)).mapTo[Either[String, List[Value]]]

  def isReady: Future[Boolean] = (hash ? Ready).mapTo[Boolean]
}