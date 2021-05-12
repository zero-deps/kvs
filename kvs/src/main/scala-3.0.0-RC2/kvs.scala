package zd.kvs

import akka.actor.*
import akka.cluster.sharding.*
import zd.kvs.store.*
import scala.concurrent.*
import zd.kvs.en.{En, EnHandler, Fd, FdHandler}
import zd.kvs.el.ElHandler
import zd.kvs.file.{File, FileHandler}

trait ReadOnlyElApi {
  def get[A: ElHandler](k: String): Res[Option[A]]
}

trait ElApi extends ReadOnlyElApi {
  def put[A: ElHandler](k: String, el: A): Res[A]
  def delete[A: ElHandler](k: String): Res[Unit]
}

trait ReadOnlyFdApi {
  def get(fd: Fd)(implicit fh: FdHandler): Res[Option[Fd]]
}

trait FdApi extends ReadOnlyFdApi {
  def put(fd: Fd)(implicit fh: FdHandler): Res[Fd]
  def delete(fd: Fd)(implicit fh: FdHandler): Res[Unit]
}

trait ReadOnlyFileApi {
  def stream(dir: String, name: String)(implicit h: FileHandler): Res[LazyList[Res[Array[Byte]]]]
  def size(dir: String, name: String)(implicit h: FileHandler): Res[Long]
}

trait FileApi extends ReadOnlyFileApi {
  def create(dir: String, name: String)(implicit h: FileHandler): Res[File]
  def append(dir: String, name: String, chunk: Array[Byte])(implicit h: FileHandler): Res[File]
  def delete(dir: String, name: String)(implicit h: FileHandler): Res[File]
  def copy(dir: String, name: (String, String))(implicit h: FileHandler): Res[File]
}

trait ReadOnlyKvs {
  val el: ReadOnlyElApi
  val fd: ReadOnlyFdApi
  val file: ReadOnlyFileApi

  def all[H <: En](fid: String, from: Option[H] = None)(implicit h: EnHandler[H]): Res[LazyList[Res[H]]]
  def get[H <: En](fid: String, id: String)(implicit h: EnHandler[H]): Res[Option[H]]
  def head[H <: En](fid: String)(implicit h: EnHandler[H]): Res[Option[H]]
}

trait WritableKvs {
  val el: ElApi
  val fd: FdApi
  val file: FileApi
}

object Kvs {
  def apply(system: ActorSystem): Kvs = rng(system)
  def rng(system: ActorSystem): Kvs = {
    val log = akka.event.Logging(system, "kvs")
    val kvs = new Kvs()(new store.Rng(system))
    val sharding = ClusterSharding(system)
    val settings = ClusterShardingSettings(system)
    sharding.start(typeName=IdCounter.shardName, entityProps=IdCounter.props(kvs.el), settings=settings,
      extractEntityId = { case msg:String => (msg,msg) },
      extractShardId = { case msg:String => (math.abs(msg.hashCode) % 100).toString }
    )
    val jmx = KvsJmx(kvs)
    jmx.registerMBean()
    CoordinatedShutdown(system).addTask("stop-kvs-jmx", "kvs jmx") { () =>
      Future {
        log.info("Unregister KVS JMX...")
        jmx.unregisterMBean()
        akka.Done
      }(using ExecutionContext.global)
    }
    kvs
  }
  def mem(): Kvs = new Kvs()(new store.Mem())
  def fs(): Kvs = ???
  def sql(): Kvs = ???
  def leveldb(): Kvs = ???
}

class Kvs(implicit dba: Dba) extends ReadOnlyKvs with WritableKvs {
  val el = new ElApi {
    def put[A: ElHandler](k: String,el: A): Res[A] = implicitly[ElHandler[A]].put(k,el)
    def get[A: ElHandler](k: String): Res[Option[A]] = implicitly[ElHandler[A]].get(k)
    def delete[A: ElHandler](k: String): Res[Unit] = implicitly[ElHandler[A]].delete(k)
  }

  val fd = new FdApi {
    def put(fd: Fd)(implicit fh: FdHandler): Res[Fd] = fh.put(fd)
    def get(fd: Fd)(implicit fh: FdHandler): Res[Option[Fd]] = fh.get(fd)
    def delete(fd: Fd)(implicit fh: FdHandler): Res[Unit] = fh.delete(fd)
  }

  def nextid(fid: String): Res[String] = dba.nextid(fid)

  def add[H <: En](el: H)(implicit h: EnHandler[H]): Res[H] = h.add(el)
  def put[H <: En](el: H)(implicit h: EnHandler[H]): Res[H] = h.put(el)
  def all[H <: En](fid: String, from: Option[H] = None)(implicit h: EnHandler[H]): Res[LazyList[Res[H]]] = h.all(fid, from)
  def get[H <: En](fid: String, id: String)(implicit h: EnHandler[H]): Res[Option[H]] = h.get(fid, id)
  def head[H <: En](fid: String)(implicit h: EnHandler[H]): Res[Option[H]] = h.head(fid)
  def remove[H <: En](fid: String, id: String)(implicit h: EnHandler[H]): Res[H] = h.remove(fid, id)
  def remove[H <: En](fid: String, ids: Seq[String])(implicit h: EnHandler[H]): Res[Vector[H]] = h.remove(fid, ids)
  def removeAfter[H <: En](en: H, cleanup: H => Res[Unit] = (_: H) => Right(()))(implicit h: EnHandler[H]): Res[Unit] = h.removeAfter(en, cleanup)
  def clearFeed[H <: En](fid: String)(implicit h: EnHandler[H]): Res[Unit] = h.clearFeed(fid)

  val file = new FileApi {
    def create(dir: String, name: String)(implicit h: FileHandler): Res[File] = h.create(dir, name)
    def append(dir: String, name: String, chunk: Array[Byte])(implicit h: FileHandler): Res[File] = h.append(dir, name, chunk)
    def stream(dir: String, name: String)(implicit h: FileHandler): Res[LazyList[Res[Array[Byte]]]] = h.stream(dir, name)
    def size(dir: String, name: String)(implicit h: FileHandler): Res[Long] = h.size(dir, name)
    def delete(dir: String, name: String)(implicit h: FileHandler): Res[File] = h.delete(dir, name)
    def copy(dir: String, name: (String, String))(implicit h: FileHandler): Res[File] = h.copy(dir, name)
  }

  object dump {
    def save(path: String): Res[String] = dba.save(path)
    def load(path: String): Res[String] = dba.load(path)
  }

  def onReady(): Future[Unit] = dba.onReady()

  def compact(): Unit = dba.compact()

  def clean(keyPrefix: Array[Byte]): Res[Unit] = dba.clean(keyPrefix)
}
