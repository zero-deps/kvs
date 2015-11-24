package mws.kvs

import mws.rng.Ack
import scala.concurrent.{Future,Await}
import scala.concurrent.duration._

trait Kvs {
  def put(key: String, str: AnyRef): Future[Ack]
  def get[T](key: String, clazz: Class[T]): Future[Option[T]]
  def delete(key: String): Future[Ack]
  def isReady: Future[Boolean]
  def close: Unit
  def entries:Iterator[String]
}

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
    def deleteFromList(key: String): Unit = 
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
