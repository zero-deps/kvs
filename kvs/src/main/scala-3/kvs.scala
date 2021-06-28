package zd.kvs

import akka.actor.*
import akka.cluster.sharding.*
import scala.concurrent.*

trait ReadableEl:
  def get[A: ElHandler](k: String): Either[Err, Option[A]]

trait WritableEl extends ReadableEl:
  def put[A: ElHandler](k: String, el: A): Either[Err, A]
  def delete[A: ElHandler](k: String): Either[Err, Unit]

trait ReadableFile:
  def stream(dir: String, name: String)(using FileHandler): Either[Err, LazyList[Either[Err, Array[Byte]]]]
  def size(dir: String, name: String)(using FileHandler): Either[Err, Long]

trait WritableFile extends ReadableFile:
  def create(dir: String, name: String)(using FileHandler): Either[Err, File]
  def append(dir: String, name: String, chunk: Array[Byte])(using FileHandler): Either[Err, File]
  def delete(dir: String, name: String)(using FileHandler): Either[Err, File]
  def copy(dir: String, name: (String, String))(using FileHandler): Either[Err, File]

class ReadableIdx(using Dba):
  type Fid = idx.IdxHandler.Fid
  type Fd = idx.IdxHandler.Fd
  type A = idx.IdxHandler.Idx
  protected val h = idx.IdxHandler
  def get(fid: Fid): Either[Err, Option[Fd]] = h.get(fid)
  def all(fid: Fid, from: Option[A]=None): Either[Err, LazyList[Either[Err, A]]] = h.all(fid, from)
  def get(fid: Fid, id: String): Either[Err, Option[A]] = h.get(fid, id)
  def getOrFail(fid: Fid, id: String): Either[Err, A] =
    get(fid, id).flatMap{
      case None => Left(KeyNotFound)
      case Some(x) => Right(x)
    }
  def exists_b(fid: Fid, id: String): Either[Err, Boolean] = h.get(fid, id).map(_.cata(_ => true, false))
  def not_exists(fid: Fid, id: String): Either[Err, Unit] = h.get(fid, id).flatMap(_.cata(_ => Left(EntryExists(id)), Right(unit)))
end ReadableIdx

class WritableIdx(using Dba) extends ReadableIdx:
  def update(fd: Fd, top: String): Either[Err, Unit] = h.update(fd, top)
  def create(fid: Fid): Either[Err, Fd] = h.create(fid)
  def delete(fid: Fid): Either[Err, Unit] = h.delete(fid)

  def add(a: A): Either[Err, A] = h.add(a)
  def put(a: A): Either[Err, A] = h.put(a)
  def remove(fid: Fid, id: String): Either[Err, Unit] = h.remove(fid, id)

trait ReadableKvs:
  val el: ReadableEl
  val file: ReadableFile
  val index: ReadableIdx

trait WritableKvs extends ReadableKvs:
  val el: WritableEl
  val file: WritableFile
  val index: WritableIdx

object Kvs:
  def apply(system: ActorSystem): Kvs = rng(system)
  def rng(system: ActorSystem): Kvs =
    val log = akka.event.Logging(system, "kvs")
    val kvs = new Kvs()(using Rng(system))
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
  def mem(): Kvs = new Kvs()(using Mem())
  def fs(): Kvs = ???
  def sql(): Kvs = ???
  def rks(dir: String): Kvs = new Kvs()(using Rks(dir))
end Kvs

class Kvs(using val dba: Dba) extends WritableKvs, AutoCloseable:
  import dba.R

  val el = new WritableEl:
    def put[A](k: String, el: A)(using h: ElHandler[A]): Either[Err, A] = h.put(k,el)
    def get[A](k: String)(using h: ElHandler[A]): Either[Err, Option[A]] = h.get(k)
    def delete[A](k: String)(using h: ElHandler[A]): Either[Err, Unit] = h.delete(k)

  val file = new WritableFile:
    def create(dir: String, name: String)(using h: FileHandler): Either[Err, File] = h.create(dir, name)
    def append(dir: String, name: String, chunk: Array[Byte])(using h: FileHandler): Either[Err, File] = h.append(dir, name, chunk)
    def stream(dir: String, name: String)(using h: FileHandler): Either[Err, LazyList[Either[Err, Array[Byte]]]] = h.stream(dir, name)
    def size(dir: String, name: String)(using h: FileHandler): Either[Err, Long] = h.size(dir, name)
    def delete(dir: String, name: String)(using h: FileHandler): Either[Err, File] = h.delete(dir, name)
    def copy(dir: String, name: (String, String))(using h: FileHandler): Either[Err, File] = h.copy(dir, name)

  val index = new WritableIdx

  object dump:
    def save(path: String): R[String] = dba.save(path)
    def load(path: String): R[String] = dba.load(path)

  def onReady(): Future[Unit] = dba.onReady()
  def compact(): Unit = dba.compact()
  def deleteByKeyPrefix(k: dba.K): R[Unit] = dba.deleteByKeyPrefix(k)

  def close(): Unit = dba.close()
end Kvs
