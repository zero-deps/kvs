package mws.kvs

import scala.util._
import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps
import akka.actor.{ActorSystem, ExtendedActorSystem, Extension, ExtensionKey}

/** Akka Extension to interact with KVS storage as built into Akka */
object Kvs extends ExtensionKey[Kvs] {
  override def lookup = Kvs
  override def createExtension(system:ExtendedActorSystem):Kvs = new Kvs(system)
}
class Kvs(system:ExtendedActorSystem) extends Extension {
  import scala.collection.JavaConverters._
  import mws.kvs.store._
  import handle._

  { /* start sharding */
    import akka.cluster.sharding._
    import mws.kvs.store.IdCounter
    val sharding = ClusterSharding(system)
    val settings = ClusterShardingSettings(system)
    sharding.start(typeName=IdCounter.shardName,entityProps=IdCounter.props,settings=settings,
      extractEntityId = { case msg:String => (msg,msg) },
      extractShardId = { case msg:String => (math.abs(msg.hashCode) % 100).toString }
    )
  }

  val cfg = system.settings.config
  val store = cfg.getString("kvs.store")
  val feeds:List[String] = cfg.getStringList("kvs.feeds").asScala.toList

  implicit val dba = system.dynamicAccess.createInstanceFor[Dba](store,
    List(classOf[ActorSystem]->system)).get

  if (cfg.getBoolean("akka.cluster.jmx.enabled")) {
    val jmx = new KvsJmx(this,system)
    jmx.createMBean()
    sys.addShutdownHook(jmx.unregisterMBean())
  }

  def put[A:ElHandler](k:String,el:A):Res[A] = implicitly[ElHandler[A]].put(k,el)
  def get[A:ElHandler](k:String):Res[A] = implicitly[ElHandler[A]].get(k)
  def delete[A:ElHandler](k:String):Res[A] = implicitly[ElHandler[A]].delete(k)

  import mws.kvs.handle.Handler._
  def put(fd:Fd):Res[Fd] = implicitly[FdHandler].put(fd)
  def get(fd:Fd):Res[Fd] = implicitly[FdHandler].get(fd)
  def delete(fd:Fd):Res[Fd] = implicitly[FdHandler].delete(fd)

  def nextid(fid:String):String = dba.nextid(fid)
  def add[H:Handler](el:H):Res[H] = implicitly[Handler[H]].add(el)
  def remove[H:Handler](el:H):Res[H] = implicitly[Handler[H]].remove(el)
  def remove[H:Handler](fid:String,id:String):Res[H] = implicitly[Handler[H]].remove(fid,id)
  def entries[H:Handler](fid:String,from:Option[H]=None,count:Option[Int]=None):Res[List[H]] = implicitly[Handler[H]].entries(fid,from,count)
  def get[H:Handler](fid:String,id:String):Res[H] = implicitly[Handler[H]].get(fid,id)

  val dump_timeout = 1 hour
  def save():Option[String] = Try(Await.result(dba.save(),dump_timeout)).toOption
  def load(path:String):Option[Any] = Try(Await.result(dba.load(path),dump_timeout)).toOption
  def iterate(path:String,foreach:(String,Array[Byte])=>Unit):Option[Any] = Try(Await.result(dba.iterate(path,foreach),dump_timeout)).toOption

  def onReady[T](body: =>Unit):Unit = {
    import scala.language.postfixOps
    import scala.concurrent.Promise
    import scala.concurrent.duration._
    import system.dispatcher
    import system.log
    val p = Promise[T]()
    val N = cfg.getIntList("ring.quorum").get(0)
    val K = if (N==1) 1 else cfg.getInt("kvs.onreadycount")
    var count = 0
    def loop():Unit = system.scheduler.scheduleOnce(1 second){
      dba.isReady onComplete {
        case Success(true) =>
          count = count + 1
          // make sure that dba is ready K times in the row
          if (count == K) {
            log.info("KVS is ready")
            body
          } else {
            log.info("KVS isn't ready yet...")
            loop()
          }
        case _ =>
          log.info("KVS isn't ready yet...")
          count = 0
          loop()
      }
    }
    loop()
  }

  def close():Unit = dba.close()
}
