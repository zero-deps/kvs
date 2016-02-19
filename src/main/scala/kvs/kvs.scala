package mws.kvs

import akka.actor.{ExtensionKey,Extension,ExtendedActorSystem}

/** Akka Extension to interact with KVS storage as built into Akka */
object Kvs extends ExtensionKey[Kvs] {
  override def lookup = Kvs
  override def createExtension(system:ExtendedActorSystem):Kvs = new Kvs(system)
}
class Kvs(system:ExtendedActorSystem) extends Extension {
  import scala.collection.JavaConversions._
  import mws.kvs.store._
  import handle._

  val cfg = system.settings.config
  val store = cfg.getString("kvs.store")
  val feeds = cfg.getStringList("kvs.feeds").toList

  implicit val dba = system.dynamicAccess.createInstanceFor[Dba](store,
    List(classOf[ExtendedActorSystem]->system)).get

  if (cfg.getBoolean("akka.cluster.jmx.enabled")) {
    val jmx = new KvsJmx(this,system.log)
    jmx.createMBean()
    sys.addShutdownHook(jmx.unregisterMBean())
  }

  def add[H:Handler](el:H):Either[Err,H] = implicitly[Handler[H]].add(el)
  def remove[H:Handler](el:H):Either[Err,H] = implicitly[Handler[H]].remove(el)
  def entries[H:Handler](fid:String,from:Option[H]=None,count:Option[Int]=None):Either[Err,List[H]] = implicitly[Handler[H]].entries(fid,from,count)

  import scala.concurrent.Future
  def isReady:Future[Boolean] = dba.isReady
  def close():Unit = dba.close()
}
