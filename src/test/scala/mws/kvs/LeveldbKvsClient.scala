package mws.kvs

import akka.actor.{Actor, ActorSystem, Props}
import org.iq80.leveldb.DBIterator
import scala.util.Try
import LeveldbKvs._

object LeveldbKvsClient extends App {
  implicit val system = ActorSystem("leveldb-client")
  val config = system.settings.config
  val kvs = new LeveldbKvs(config.getConfig("leveldb"), system.log)
  val presenter = system.actorOf(Props(new Presenter(kvs)))
  presenter ! "all"
}

class Presenter(kvs: LeveldbKvs) extends Actor {
  def receive: Receive = {
    case "all" =>
      val all = getAll(kvs)
      val table = Seq(List("Key", "Value")) ++ all.map { case (k, v) => List(k, v) }
      println(Tabulator.format(table))
      kvs.close()
      context.system.shutdown()
  }

  def getAll(kvs: LeveldbKvs): List[(String, String)] = {
    val db = kvs.leveldb
    val ro = kvs.leveldbReadOptions.snapshot(db.getSnapshot)
    try {
      val it = db.iterator(ro)
      try {
        it.seekToFirst()
        iteratorToList(it, acc = Nil)
      } finally Try(it.close())
    } finally Try(ro.snapshot().close())
  }

  def iteratorToList(it: DBIterator, acc: List[(String, String)])
    : List[(String, String)] = {
    if (it.hasNext) {
      val k: String = it.peekNext().getKey
      val v: String = it.peekNext().getValue
      it.next()
      iteratorToList(it, (k, v) :: acc)
    } else acc
  }
}

object Tabulator {
  def format(table: Seq[Seq[Any]]) = table match {
    case Seq() => ""
    case _ =>
      val sizes = for (row <- table) yield (for (cell <- row) yield if (cell == null) 0 else cell.toString.length)
      val colSizes = for (col <- sizes.transpose) yield col.max
      val rows = for (row <- table) yield formatRow(row, colSizes)
      formatRows(rowSeparator(colSizes), rows)
  }

  def formatRows(rowSeparator: String, rows: Seq[String]): String = (
    rowSeparator ::
    rows.head ::
    rowSeparator ::
    rows.tail.toList :::
    rowSeparator ::
    List()
  ).mkString("\n")

  def formatRow(row: Seq[Any], colSizes: Seq[Int]) = {
    val cells = (for ((item, size) <- row.zip(colSizes)) yield if (size == 0) "" else ("%-" + size + "s").format(item))
    cells.mkString("|", "|", "|")
  }

  def rowSeparator(colSizes: Seq[Int]) = colSizes map { "-" * _ } mkString("+", "+", "+")
}
