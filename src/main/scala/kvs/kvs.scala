package mws.kvs

import store._

import akka.actor.{ActorSystem, ExtensionKey, Extension, ExtendedActorSystem}
import com.typesafe.config.Config

import mws.rng._
import scala.concurrent.{Future,Await}
import scala.concurrent.duration._

/** 
 * Akka Extension to interact with KVS storage as built into Akka.
 */
object Kvs extends ExtensionKey[Kvs] {
  override def lookup = Kvs
  override def createExtension(system: ExtendedActorSystem): Kvs = new Kvs(system)
}
class Kvs(val system: ExtendedActorSystem) extends Extension {
  val c = system.settings.config
  val kvsCfg = c.getConfig("kvs")
  val store = kvsCfg.getString("store")

  import scala.collection._

  implicit val dba:Dba = system.dynamicAccess.createInstanceFor[Dba](store,
    immutable.Seq(classOf[Config]-> c.getConfig("leveldb"))).get

  def put[H:Handler](el:H):Either[Err,H] = implicitly[Handler[H]].put(el)

  def get[H: Handler](k:String):Either[Err,H] = implicitly[Handler[H]].get(k)

  def delete[H:Handler](key: String): Either[Err,H] = implicitly[Handler[H]].delete(key)

  def isReady: Future[Boolean] = dba.isReady
  def close:Unit = dba.close
  def config:Config = kvsCfg
}

trait Iterable { self: Kvs =>
  private val First = "first"
  private val Last = "last"
  private val Sep = ";"

//  def add[T](container:String, el: T): Either[Throwable, T]
//  def remove[T](container:String, el:T): Either[Throwable, T]
//  def entries:Iterator[String]


/*  def putToList(item: Data): Unit = {
    val key = item.key
    val data = item.serialize
    getEntry(key) match {
      case Some(Entry(_, prev, next)) =>
        self.put(key, Entry(data, prev, next))
      case _ =>
        Await.result(self.get[String](Last),1 second) match {
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
*/
}
