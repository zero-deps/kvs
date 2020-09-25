package zd.kvs

import akka._, actor._
import scala.concurrent._
import zd.kvs.el.ElHandler
import zd.kvs.en.{En, IdEn, EnHandler, Fd, FdHandler}
import zd.kvs.file.{File, FileHandler}
import zd.kvs.store._
import zero.ext._, option._
import zd.proto.Bytes

trait ReadOnlyElApi {
  def get(k: Bytes): Res[Option[Bytes]]
}

trait ElApi extends ReadOnlyElApi {
  def put(k: Bytes, v: Bytes): Res[Unit]
  def delete(k: Bytes): Res[Unit]
}

trait ReadOnlyFdApi {
  def get(id: Bytes): Res[Option[Fd]]
  def length(id: Bytes): Res[Long]
}

trait FdApi extends ReadOnlyFdApi {
  def put(fd: Fd): Res[Unit]
  def delete(id: Bytes): Res[Unit]
}

trait ReadOnlyFileApi {
  def stream(dir: Bytes, name: Bytes)(implicit h: FileHandler): Res[LazyList[Res[Bytes]]]
  def size(dir: Bytes, name: Bytes)(implicit h: FileHandler): Res[Long]
}

trait FileApi extends ReadOnlyFileApi {
  def create(dir: Bytes, name: Bytes)(implicit h: FileHandler): Res[File]
  def append(dir: Bytes, name: Bytes, chunk: Bytes)(implicit h: FileHandler): Res[File]
  def delete(dir: Bytes, name: Bytes)(implicit h: FileHandler): Res[File]
  def copy(dir: Bytes, name: (Bytes, Bytes))(implicit h: FileHandler): Res[File]
}

trait ReadOnlyKvs {
  val el: ReadOnlyElApi
  val fd: ReadOnlyFdApi
  val file: ReadOnlyFileApi

  def all(fid: Bytes, next: Option[Option[Bytes]]=none, removed: Boolean=false): Res[LazyList[Res[IdEn]]]
  def all(fd: Fd, next: Option[Option[Bytes]], removed: Boolean): LazyList[Res[IdEn]]
  def get(fid: Bytes, id: Bytes): Res[Option[En]]
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
    def put(k: Bytes, v: Bytes): Res[Unit] = ElHandler.put(k, v)
    def get(k: Bytes): Res[Option[Bytes]] = ElHandler.get(k)
    def delete(k: Bytes): Res[Unit] = ElHandler.delete(k)
  }

  val fd = new FdApi {
    def put(fd: Fd): Res[Unit] = FdHandler.put(fd)
    def get(id: Bytes): Res[Option[Fd]] = FdHandler.get(id)
    def delete(id: Bytes): Res[Unit] = FdHandler.delete(id)
    def length(id: Bytes): Res[Long] = FdHandler.length(id)
  }

  def add(fid: Bytes, data: Bytes): Res[IdEn] = EnHandler.prepend(fid, data)
  def add(fid: Bytes, id: Bytes, data: Bytes): Res[En] = EnHandler.prepend(fid, id, data)
  def put(fid: Bytes, id: Bytes, data: Bytes): Res[En] = EnHandler.put(fid, id, data)
  def all(fid: Bytes, next: Option[Option[Bytes]]=none, removed: Boolean=false): Res[LazyList[Res[IdEn]]] = EnHandler.all(fid, next, removed)
  def all(fd: Fd, next: Option[Option[Bytes]], removed: Boolean): LazyList[Res[IdEn]] = EnHandler.all(fd, next, removed)
  def get(fid: Bytes, id: Bytes): Res[Option[En]] = EnHandler.get(fid, id)
  def head(fid: Bytes): Res[Option[En]] = EnHandler.head(fid)
  def remove(fid: Bytes, id: Bytes): Res[Option[En]] = EnHandler.remove_soft(fid, id)
  def cleanup(fid: Bytes): Res[Unit] = EnHandler.cleanup(fid)
  def fix(fid: Bytes): Res[((Long,Long),(Long,Long),(Bytes,Bytes))] = EnHandler.fix(fid)

  val file = new FileApi {
    def create(dir: Bytes, name: Bytes)(implicit h: FileHandler): Res[File] = h.create(dir, name)
    def append(dir: Bytes, name: Bytes, chunk: Bytes)(implicit h: FileHandler): Res[File] = h.append(dir, name, chunk)
    def stream(dir: Bytes, name: Bytes)(implicit h: FileHandler): Res[LazyList[Res[Bytes]]] = h.stream(dir, name)
    def size(dir: Bytes, name: Bytes)(implicit h: FileHandler): Res[Long] = h.size(dir, name)
    def delete(dir: Bytes, name: Bytes)(implicit h: FileHandler): Res[File] = h.delete(dir, name)
    def copy(dir: Bytes, name: (Bytes, Bytes))(implicit h: FileHandler): Res[File] = h.copy(dir, name)
  }

  object dump {
    def save(path: String): Res[String] = dba.save(path)
    def load(path: String): Res[Any] = dba.load(path)
  }

  def onReady(): Future[Unit] = dba.onReady()

  def compact(): Unit = dba.compact()
}
