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

  def put[H:Handler](k:String,el:H):Either[Err,H] = dba.put(k,implicitly[Handler[H]].pickle(el)).right.map(_=>el)
  def get[H:Handler](k:String):Either[Err,H] = dba.get(k).right.map(implicitly[Handler[H]].unpickle)
  def delete[H:Handler](k:String):Either[Err,H] = dba.delete(k).right.map(implicitly[Handler[H]].unpickle)

  def put[Fd:Handler](fd:Fd):Either[Err,Fd] = implicitly[Handler[Fd]].put(fd)
  def add[H:Handler](el:H):Either[Err,H] = implicitly[Handler[H]].add(el)
  def remove[H:Handler](el:H):Either[Err,H] = implicitly[Handler[H]].remove(el)
  def entries[H:Handler](fid:String):Either[Err,List[H]] = entries(fid,None,None)
  def entries[H:Handler](fid:String,from:Option[H],count:Option[Int]):Either[Err,List[H]] = implicitly[Handler[H]].entries(fid,from,count)

  def save(): Unit = dba.save()
  def load(dumpPath: String) = dba.load(dumpPath)

  import scala.concurrent.Future
  def onReady[T](body: =>T):Future[T] = {
    import scala.language.postfixOps
    import scala.concurrent.Promise
    import scala.concurrent.duration._
    import system.dispatcher
    import system.log
    val p = Promise[T]()
    def loop():Unit =
      system.scheduler.scheduleOnce(1 second){
        dba.isReady.map{
          case true =>
            log.info("KVS is ready")
            p success body
          case false =>
            log.info("KVS isn't ready yet...")
            loop()
        }
      }
    loop()
    p.future
  }

  def close():Unit = dba.close()
}
