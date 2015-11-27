package mws.kvs

import akka.actor.{ActorSystem, ExtensionKey, Extension, ExtendedActorSystem}
import com.typesafe.config.Config

import mws.rng.Ack
import scala.concurrent.{Future,Await}
import scala.concurrent.duration._

/**
 *:create_schema(node()) ok
 * create_tables(name, [att, info(record)]), add_table_index
 *:start ok
 *:stop ok
 *:destroy 
 *:join
 *:version
 *:dir 
 * info()-> processes, transactions, schema, etc.
 *:config
 * ---------
 *:containers
 *:create
 *:link
 *:add
 *:delete
 *:traversal
 *:entries
 *:put
 *:get
 *:all
 *:index
 *:next_id
 *:save_db
 *:load_db
 *:dump
 *-------
 *:id_sec
 *:last_disc  ^
 *:last_table ^
 *:update_cfg ^
 * store handlers
 * - same with modified data
 *
 *
 */
trait Kvs {
  def put(key: String, str: AnyRef): Future[Ack]
  def get[T](key: String, clazz: Class[T]): Future[Option[T]]
  def delete(key: String): Future[Ack]
  def isReady: Future[Boolean]
  def close: Unit
  def entries:Iterator[String]
  def add[T](container:String, el: T): Either[Throwable, T]
  def remove[T](container:String, el:T): Either[Throwable, T]
}

/** Akka Extension to interact with KVS storage as built into Akka */
object KvsExt extends ExtensionKey[KvsExt] {
  override def lookup = KvsExt
  override def createExtension(system: ExtendedActorSystem): KvsExt = new KvsExt(system)
}
class KvsExt(val system: ExtendedActorSystem) extends Extension with Kvs {
  val c = system.settings.config
  val kvsc = c.getConfig("kvs")
  val store = kvsc.getString("store")

  import scala.collection._

  val storeArgs = immutable.Seq(classOf[ActorSystem]-> system)
  implicit val dba:Kvs = system.dynamicAccess.createInstanceFor[Kvs](store, storeArgs).get

  def add[T](container: String,el: T): Either[Throwable,T] = dba.add(container,el)
  def close: Unit = dba.close
  def delete(key: String): scala.concurrent.Future[mws.rng.Ack] = dba.delete(key)
  def entries: Iterator[String] = dba.entries
  def get[T](key: String,clazz: Class[T]): scala.concurrent.Future[Option[T]] = dba.get(key,clazz)
  def isReady: scala.concurrent.Future[Boolean] = dba.isReady
  def put(key: String,str: AnyRef): scala.concurrent.Future[mws.rng.Ack] = dba.put(key,str)
  def remove[T](container: String,el: T): Either[Throwable,T] = dba.remove(container,el)

}
//
trait TypedKvs[T <: AnyRef] {
  def put(key: String, value: T): Either[Throwable, T]
  def get(key: String): Either[Throwable, Option[T]]
  def remove(key: String): Either[Throwable, Option[T]]

  protected def clazz: Class[T]
}

trait Iterable { self: Kvs =>
  private val First = "first"
  private val Last = "last"
  private val Sep = ";"

  def putToList(item: Data): Unit = {
    val key = item.key
    val data = item.serialize
    getEntry(key) match {
      case Some(Entry(_, prev, next)) =>
        self.put(key, Entry(data, prev, next))
      case _ =>
        Await.result(self.get[String](Last,classOf[String]),1 second) match {
          case Some(lastKey) =>
            // insert last
            self.put(key, Entry(data, prev = Some(lastKey), next = None))
            // link prev to last
            val last = getEntry(lastKey).get
            self.put(lastKey, last.copy(next = Some(key)))
            // update link to last
            self.put(Last, key)
          case None =>
            self.put(key, Entry(data, prev = None, next = None))
            self.put(First, key)
            self.put(Last, key)
        }
    }
  }

  def entries: Iterator[String] =
    Iterator.iterate {
      val k = Await.result(self.get[String](First,classOf[String]),1 second)
      k.flatMap(getEntry)
    } { _v =>
      val k = _v.flatMap(_.next)
      k.flatMap(getEntry)
    } takeWhile(_.isDefined) map(_.get.data)

  def deleteFromList(el: Data): Unit = {
    val key = el.key
    getEntry(key) map { case Entry(_, prev, next) =>
      prev match {
        case Some(prev) =>
          getEntry(prev) map { entry =>
            self.put(prev, entry.copy(next = next))
          }
        case _ =>
      }
      next match {
        case Some(next) =>
          getEntry(next) map { entry =>
            self.put(next, entry.copy(prev = prev))
          }
        case _ =>
      }
      (Await.result(self.get[String](First,classOf[String]),1 second),
       Await.result(self.get[String](Last,classOf[String]),1 second)) match {
        case (Some(`key`), Some(`key`)) =>
          self.delete(First)
          self.delete(Last)
        case (Some(`key`), _) =>
          self.put(First, next.get)
        case (_, Some(`key`)) =>
          self.put(Last, prev.get)
        case _ =>
      }
      self.delete(key)
  }
  }

  def first: Option[String] = Await.result(self.get[String](First,classOf[String]), 1 second).flatMap(getEntry).map(_.data)
  def last: Option[String] = Await.result(self.get[String](Last,classOf[String]),1 second).flatMap(getEntry).map(_.data)

  private def getEntry(key: String): Option[Entry] =
    Await.result(self.get[String](key,classOf[String]),1 second) map { entry =>
      val xs = entry.split(Sep, -1)
      val data = xs(0)
      val prev = if (xs(1) != "") Some(xs(1)) else None
      val next = if (xs(2) != "") Some(xs(2)) else None
      Entry(data, prev, next)
    }
  implicit private def serialize(entry: Entry): String =
    List(
      entry.data,
      entry.prev.getOrElse(""),
      entry.next.getOrElse("")
    ).mkString(Sep)
}
