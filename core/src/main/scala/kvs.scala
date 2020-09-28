package zd.kvs

import akka._, actor._
import scala.concurrent._
import zd.kvs.el.ElHandler
import zd.kvs.en.{En, `Key,En`, EnHandler, Fd, FdHandler}
import zd.kvs.file.{File, FileHandler}
import zd.kvs.store._
import zero.ext._, option._
import zd.proto.Bytes

trait ReadOnlyElApi {
  def get(k: ElKey): Res[Option[Bytes]]
}

trait ElApi extends ReadOnlyElApi {
  def put(k: ElKey, v: Bytes): Res[Unit]
  def delete(k: ElKey): Res[Unit]
}

trait ReadOnlyFdApi {
  def get(id: FdKey): Res[Option[Fd]]
  def length(id: FdKey): Res[Long]
}

trait FdApi extends ReadOnlyFdApi {
  def put(id: FdKey, fd: Fd): Res[Unit]
  def delete(id: FdKey): Res[Unit]
}

trait ReadOnlyFileApi {
  def stream(path: PathKey)(implicit h: FileHandler): Res[LazyList[Res[Bytes]]]
  def size(path: PathKey)(implicit h: FileHandler): Res[Long]
}

trait FileApi extends ReadOnlyFileApi {
  def create(path: PathKey)(implicit h: FileHandler): Res[File]
  def append(path: PathKey, chunk: Bytes)(implicit h: FileHandler): Res[File]
  def delete(path: PathKey)(implicit h: FileHandler): Res[File]
  def copy(from: PathKey, to: PathKey)(implicit h: FileHandler): Res[File]
}

trait ReadOnlyKvs {
  val el: ReadOnlyElApi
  val fd: ReadOnlyFdApi
  val file: ReadOnlyFileApi

  def all(fid: FdKey, next: Option[Option[Bytes]]=none, removed: Boolean=false): Res[LazyList[Res[`Key,En`]]]
  def get(key: EnKey): Res[Option[En]]
  def apply(key: EnKey): Res[En]
}

object Kvs {
  def apply(system: ActorSystem): Kvs = rng(system)
  def rng(system: ActorSystem): Kvs = {
    val kvs = new Kvs()(new store.Rng(system))
    if (system.settings.config.getBoolean("akka.cluster.jmx.enabled")) {
      val jmx = new KvsJmx(kvs)
      jmx.createMBean()
      sys.addShutdownHook(jmx.unregisterMBean())
    }
    kvs
  }
  def mem(): Kvs = new Kvs()(new store.Mem())
  def fs(): Kvs = ???
  def sql(): Kvs = ???
  def leveldb(): Kvs = ???
}

class Kvs(implicit val dba: Dba) extends ReadOnlyKvs {
  val el = new ElApi {
    def put(k: ElKey, v: Bytes): Res[Unit] = ElHandler.put(k, v)
    def get(k: ElKey): Res[Option[Bytes]] = ElHandler.get(k)
    def delete(k: ElKey): Res[Unit] = ElHandler.delete(k)
  }

  val fd = new FdApi {
    def put(fid: FdKey, fd: Fd): Res[Unit] = FdHandler.put(fid, fd)
    def get(id: FdKey): Res[Option[Fd]] = FdHandler.get(id)
    def delete(id: FdKey): Res[Unit] = FdHandler.delete(id)
    def length(id: FdKey): Res[Long] = FdHandler.length(id)
  }

  def add(fid: FdKey, data: Bytes): Res[`Key,En`] = EnHandler.prepend(fid, data)
  def add(key: EnKey, data: Bytes): Res[En] = EnHandler.prepend(key, data)
  def put(key: EnKey, data: Bytes): Res[En] = EnHandler.put(key, data)
  def all(fid: FdKey, next: Option[Option[Bytes]]=none, removed: Boolean=false): Res[LazyList[Res[`Key,En`]]] = EnHandler.all(fid, next, removed)
  def get(key: EnKey): Res[Option[En]] = EnHandler.get(key)
  def apply(key: EnKey): Res[En] = EnHandler.apply(key)
  def head(fid: FdKey): Res[Option[`Key,En`]] = EnHandler.head(fid)
  def remove(key: EnKey): Res[Option[En]] = EnHandler.remove_soft(key)
  def cleanup(fid: FdKey): Res[Unit] = EnHandler.cleanup(fid)
  def fix(fid: FdKey): Res[((Long,Long),(Long,Long),(Bytes,Bytes))] = EnHandler.fix(fid)

  val file = new FileApi {
    def create(path: PathKey)(implicit h: FileHandler): Res[File] = h.create(path)
    def append(path: PathKey, chunk: Bytes)(implicit h: FileHandler): Res[File] = h.append(path, chunk)
    def stream(path: PathKey)(implicit h: FileHandler): Res[LazyList[Res[Bytes]]] = h.stream(path)
    def size(path: PathKey)(implicit h: FileHandler): Res[Long] = h.size(path)
    def delete(path: PathKey)(implicit h: FileHandler): Res[File] = h.delete(path)
    def copy(from: PathKey, to: PathKey)(implicit h: FileHandler): Res[File] = h.copy(fromPath=from, toPath=to)
  }

  object dump {
    def save(path: String): Res[String] = dba.save(path)
    def load(path: String): Res[Any] = dba.load(path)
  }

  def onReady(): Future[Unit] = dba.onReady()

  def compact(): Unit = dba.compact()
}
