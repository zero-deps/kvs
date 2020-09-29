package zd.kvs

import akka._, actor._
import scala.concurrent._
import zd.kvs.el.ElHandler
import zd.kvs.en.{EnHandler, Fd, FdHandler}
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

  def all[A: Entry](next: Option[ElKey]=none): Res[LazyList[Res[(ElKey, A)]]]
  def get[A: Entry](key: ElKey): Res[Option[A]]
  def apply[A: Entry](key: ElKey): Res[A]
  def head[A: Entry](): Res[Option[(ElKey, A)]]
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

trait Entry[A] {
  def fid(): FdKey
  def extract(xs: Bytes): A
  def insert(x: A): Bytes
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

  def add[A: Entry](data: A): Res[ElKey] = {
    val en = implicitly[Entry[A]]
    EnHandler.prepend(en.fid, en.insert(data))
  }
  def add[A: Entry](key: ElKey, data: A): Res[Unit] = {
    val en = implicitly[Entry[A]]
    EnHandler.prepend(EnKey(en.fid, key), en.insert(data))
  }
  def put[A: Entry](key: ElKey, data: A): Res[Unit] = {
    val en = implicitly[Entry[A]]
    EnHandler.put(EnKey(en.fid, key), en.insert(data))
  }
  def all[A: Entry](next: Option[ElKey]=none): Res[LazyList[Res[(ElKey, A)]]] = {
    val en = implicitly[Entry[A]]
    EnHandler.all(en.fid, next.map(_.some), removed=false).map(_.map(_.map{ x => x._1 -> en.extract(x._2.data) }))
  }
  def get[A: Entry](key: ElKey): Res[Option[A]] = {
    val en = implicitly[Entry[A]]
    EnHandler.get(EnKey(en.fid, key)).map(_.map(x => en.extract(x)))
  }
  def apply[A: Entry](key: ElKey): Res[A] = {
    val en = implicitly[Entry[A]]
    EnHandler.apply(EnKey(en.fid, key)).map(x => en.extract(x))
  }
  def head[A: Entry](): Res[Option[(ElKey, A)]] = {
    val en = implicitly[Entry[A]]
    EnHandler.head(en.fid).map(_.map(x => x._1 -> en.extract(x._2)))
  }
  def remove[A: Entry](key: ElKey): Res[Option[A]] = {
    val en = implicitly[Entry[A]]
    EnHandler.remove(EnKey(en.fid, key)).map(_.map(x => en.extract(x.data)))
  }
  def cleanup(fid: FdKey): Res[Unit] = EnHandler.cleanup(fid)
  def fix(fid: FdKey): Res[((Long,Long),(Long,Long),(ElKey,ElKey))] = EnHandler.fix(fid)

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
