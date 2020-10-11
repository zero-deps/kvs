package kvs

import scala.concurrent._
import akka._, actor._
import zero.ext._, option._
import zd.proto.Bytes

import store.Dba, el.ElHandler, en.{EnHandler, Fd, FdHandler}, file.{File, FileHandler}

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

  def all  [A: DataCodec](fid: FdKey, after: Option[ElKey]=none): Res[LazyList[Res[(ElKey, A)]]]
  def get  [A: DataCodec](fid: FdKey, id: ElKey): Res[Option[A]]
  def apply[A: DataCodec](fid: FdKey, id: ElKey): Res[A]
  def head [A: DataCodec](fid: FdKey): Res[Option[A]]
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

trait DataCodec[A] {
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

  def add[A: DataCodec](fid: FdKey, data: A): Res[ElKey] = {
    val en = implicitly[DataCodec[A]]
    EnHandler.prepend(fid, en.insert(data))
  }
  def add[A: DataCodec](fid: FdKey, id: ElKey, data: A): Res[Unit] = {
    val en = implicitly[DataCodec[A]]
    EnHandler.prepend(EnKey(fid, id), en.insert(data))
  }
  def put[A: DataCodec](fid: FdKey, id: ElKey, data: A): Res[Unit] = {
    val en = implicitly[DataCodec[A]]
    EnHandler.put(EnKey(fid, id), en.insert(data))
  }
  def all[A: DataCodec](fid: FdKey, after: Option[ElKey]=none): Res[LazyList[Res[(ElKey, A)]]] = {
    val en = implicitly[DataCodec[A]]
    EnHandler.all(fid, after.map(_.some), removed=false).map(_.map(_.map{ x => x._1 -> en.extract(x._2.data) }))
  }
  def get[A: DataCodec](fid: FdKey, id: ElKey): Res[Option[A]] = {
    val en = implicitly[DataCodec[A]]
    EnHandler.get(EnKey(fid, id)).map(_.map(x => en.extract(x)))
  }
  def apply[A: DataCodec](fid: FdKey, id: ElKey): Res[A] = {
    val en = implicitly[DataCodec[A]]
    EnHandler.apply(EnKey(fid, id)).map(x => en.extract(x))
  }
  def head[A: DataCodec](fid: FdKey): Res[Option[A]] = {
    val en = implicitly[DataCodec[A]]
    EnHandler.head(fid).map(_.map(x => en.extract(x._2)))
  }
  def remove[A: DataCodec](fid: FdKey, id: ElKey): Res[Option[A]] = {
    val en = implicitly[DataCodec[A]]
    EnHandler.remove(EnKey(fid, id)).map(_.map(x => en.extract(x.data)))
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
