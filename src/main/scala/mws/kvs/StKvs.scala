package mws.kvs

import scala.language.implicitConversions

trait StKvs {
  def put(key: String, value: String): Unit
  def get(key: String): Option[String]
  def delete(key: String): Unit
  def close(): Unit
}

object StKvs {
  class Wrapper(kvs: StKvs, namespace: String) extends StKvs {
    def put(key: String, value: String): Unit = kvs.put(s"$namespace.$key", value)
    def get(key: String): Option[String] = kvs.get(s"$namespace.$key")
    def delete(key: String): Unit = kvs.delete(s"$namespace.$key")
    def close(): Unit = kvs.close()
  }

  trait Data {
    def key: String
    def serialize: String
  }

  trait Iterable { kvs: StKvs =>
    private val First = "first"
    private val Last = "last"
    private val Sep = ";"

    private case class Entry(data: String, prev: Option[String], next: Option[String])

    def putToList(item: Data): Unit = {
      val key = item.key
      val data = item.serialize
      getEntry(key) match {
        case Some(Entry(_, prev, next)) =>
          kvs.put(key, Entry(data, prev, next))
        case _ =>
          kvs.get(Last) match {
            case Some(lastKey) =>
              // insert last
              kvs.put(key, Entry(data, prev = Some(lastKey), next = None))
              // link prev to last
              val last = getEntry(lastKey).get
              kvs.put(lastKey, last.copy(next = Some(key)))
              // update link to last
              kvs.put(Last, key)
            case None =>
              kvs.put(key, Entry(data, prev = None, next = None))
              kvs.put(First, key)
              kvs.put(Last, key)
          }
      }
    }

    def values: Iterator[String] =
      Iterator.iterate {
        val k = kvs.get(First)
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
              kvs.put(prev, entry.copy(next = next))
            }
          case _ =>
        }
        next match {
          case Some(next) =>
            getEntry(next) map { entry =>
              kvs.put(next, entry.copy(prev = prev))
            }
          case _ =>
        }
        (kvs.get(First), kvs.get(Last)) match {
          case (Some(`key`), Some(`key`)) =>
            kvs.delete(First)
            kvs.delete(Last)
          case (Some(`key`), _) =>
            kvs.put(First, next.get)
          case (_, Some(`key`)) =>
            kvs.put(Last, prev.get)
          case _ =>
        }
        kvs.delete(key)
      }
    
    def first: Option[String] = kvs.get(First).flatMap(getEntry).map(_.data)

    def last: Option[String] = kvs.get(Last).flatMap(getEntry).map(_.data)

    private def getEntry(key: String): Option[Entry] =
      kvs.get(key) map { entry =>
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
}
