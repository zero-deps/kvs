package mws.kvs

import akka.actor.{ActorSystem, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.cluster.sharding._
import mws.kvs.store._
import mws.kvs.store.IdCounter
import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try
import scalaz._
import scalaz.Scalaz._
import mws.kvs.en.{En, EnHandler, Fd, FdHandler}
import mws.kvs.el.ElHandler
import mws.kvs.file.{File, FileHandler}

/** Akka Extension to interact with KVS storage as built into Akka */
object Kvs extends ExtensionId[Kvs] with ExtensionIdProvider {
  override def lookup = Kvs
  override def createExtension(system:ExtendedActorSystem):Kvs = new Kvs(system)
}
class Kvs(system:ExtendedActorSystem) extends Extension {
  { /* start sharding */
    val sharding = ClusterSharding(system)
    val settings = ClusterShardingSettings(system)
    sharding.start(typeName=IdCounter.shardName,entityProps=IdCounter.props,settings=settings,
      extractEntityId = { case msg:String => (msg,msg) },
      extractShardId = { case msg:String => (math.abs(msg.hashCode) % 100).toString }
    )
  }

  val cfg = system.settings.config
  val store = cfg.getString("kvs.store")

  implicit val dba = system.dynamicAccess.createInstanceFor[Dba](store,
    List(classOf[ActorSystem]->system)).get

  if (cfg.getBoolean("akka.cluster.jmx.enabled")) {
    val jmx = new KvsJmx(this,system)
    jmx.createMBean()
    sys.addShutdownHook(jmx.unregisterMBean())
  }

  object el {
    def put[A: ElHandler](k: String,el: A): Res[A] = implicitly[ElHandler[A]].put(k,el)
    def get[A: ElHandler](k: String): Res[A] = implicitly[ElHandler[A]].get(k)
    def delete[A: ElHandler](k: String): Res[A] = implicitly[ElHandler[A]].delete(k)
  }

  object fd {
    def put(fd: Fd)(implicit fh: FdHandler): Res[Fd] = fh.put(fd)
    def get(fd: Fd)(implicit fh: FdHandler): Res[Fd] = fh.get(fd)
    def delete(fd: Fd)(implicit fh: FdHandler): Res[Fd] = fh.delete(fd)
  }

  def nextid(fid: String): Res[String] = dba.nextid(fid)

  def add[H <: En](el: H)(implicit h: EnHandler[H]): Res[H] = h.add(el)
  def put[H <: En](el: H)(implicit h: EnHandler[H]): Res[H] = h.put(el)
  def stream[H <: En](fid: String, from: Option[H] = None)(implicit h: EnHandler[H]): Res[Stream[Res[H]]] = h.stream(fid, from)
  def get[H <: En](fid: String, id: String)(implicit h: EnHandler[H]): Res[H] = h.get(fid, id)
  def remove[H <: En](fid: String, id: String)(implicit h: EnHandler[H]): Res[H] = h.remove(fid, id)

  object file {
    def create(dir: String, name: String)(implicit h: FileHandler): Res[File] = h.create(dir, name)
    def append(dir: String, name: String, chunk: Array[Byte])(implicit h: FileHandler): Res[File] = h.append(dir, name, chunk)
    def stream(dir: String, name: String)(implicit h: FileHandler): Res[Stream[Res[Array[Byte]]]] = h.stream(dir, name)
    def size(dir: String, name: String)(implicit h: FileHandler): Res[Long] = h.size(dir, name)
  }

  object dump {
    private val timeout = 1 hour
    def save(path: String): Res[String] = Try(Await.result(dba.save(path), timeout)).toDisjunction.leftMap(Failed)
    def load(path: String): Res[Any] = Try(Await.result(dba.load(path), timeout)).toDisjunction.leftMap(Failed)
    def iterate(path: String, f: (String, Array[Byte]) => Unit): Res[Any] = Try(Await.result(dba.iterate(path, f), timeout)).toDisjunction.leftMap(Failed)
  }

  def onReady[T](body: => Unit): Unit = {
    import system.dispatcher
    import system.log
    val N = cfg.getIntList("ring.quorum").get(0)
    val K = if (N==1) 1 else cfg.getInt("kvs.onreadycount")
    var count = 0
    def loop(): Unit = system.scheduler.scheduleOnce(1 second){
      dba.isReady onComplete {
        case scala.util.Success(true) =>
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

  def close(): Unit = dba.close()
}
