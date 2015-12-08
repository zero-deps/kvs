package mws.kvs.importer

import java.io.File
import akka.actor._
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.fusesource.leveldbjni.JniDBFactory
import org.iq80.leveldb.Options

object LevelDbImport {
  def props(dbPath: String, service: ActorRef) = Props(classOf[LevelDbImport], dbPath, service)
}

class LevelDbImport(val path: String, service: ActorRef) extends Actor with ActorLogging{

  private val dbFile: File = new File(path)
  if(!dbFile.exists()) log.warning(s"Leveldb directory ($path) is invalid. Import will not be performed")
  val factory = JniDBFactory.factory
  val options = new Options().createIfMissing(true)
  val db = factory.open(dbFile, options)
  val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  val itr = db.iterator
  itr.seekToFirst()

  override def receive: Actor.Receive = {
/*    case StartImport if dbFile.exists =>
      while (itr.hasNext) {
        val entry = itr.next()
        val userId = new String(entry.getKey)
        val games = Option(new String(entry.getValue)) match {
          case Some(json) => mapper.readValue(json, new TypeReference[List[Map[String, String]]] {})
          case None => List()
        }
        service ! PutGames(games, userId)
      }

      db.close()
      self ! PoisonPill
  */

    case _ =>
  }
}
