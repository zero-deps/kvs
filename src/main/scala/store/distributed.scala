package mws.kvs
package store

import java.io.File
import java.util.concurrent.TimeUnit

import akka.event.Logging
import akka.routing.FromConfig
import mws.rng.store.{ReadonlyStore, WriteStore}
import org.iq80.leveldb.{CompressionType, Options}
import akka.pattern.ask
import scala.concurrent.duration._
import scala.util._
import akka.actor.{ActorSystem, Deploy, Props}
import akka.util.{Timeout, ByteString}
import scala.concurrent.{Await, Future}
import mws.rng._

object Ring {
  def apply(system: ActorSystem): Dba = new Ring(system)

  def openLeveldb(s: ActorSystem, path: Option[String]= None) = {
    val config = s.settings.config.getConfig("ring.leveldb")
    val nativeLeveldb: Boolean = sys.props.get("os.name") match {
      case Some(os) if os.startsWith("Windows") => false //Forcing usage of Java ported LevelDB for Windows OS"
      case _ => config.getBoolean("native")
    }
    val leveldbOptions = new Options().createIfMissing(true)
    val leveldbDir = new File(path.getOrElse(config.getString("dir")))
    val factory = if (nativeLeveldb) org.fusesource.leveldbjni.JniDBFactory.factory

    else org.iq80.leveldb.impl.Iq80DBFactory.factory
      factory.open(leveldbDir, if (nativeLeveldb) leveldbOptions else leveldbOptions.compressionType(CompressionType.NONE))
  }
}

class Ring(system: ActorSystem) extends Dba {
  import Ring._
  lazy val log = Logging(system, "hash-ring")
  implicit val timeout = Timeout(5.second)
  val d = Duration(5, TimeUnit.SECONDS)

  lazy val clusterConfig = system.settings.config.getConfig("akka.cluster")
  system.eventStream

  var leveldb = openLeveldb(system)

  system.actorOf(Props(classOf[WriteStore],leveldb).withDeploy(Deploy.local), name="ring_write_store")
  system.actorOf(FromConfig.props(Props(classOf[ReadonlyStore], leveldb)).withDeploy(Deploy.local), name = "ring_readonly_store")

  private val hash = system.actorOf(Props(classOf[Hash]).withDeploy(Deploy.local), name = "ring_hash")

  def put(key: String, value: V): Either[Err, V] = {
    val putF = (hash ? Put(key, ByteString(value))).mapTo[Ack]
    Try(Await.result(putF, d)) match {
      case Success(AckSuccess) => Right(value)
      case Success(not_success: Ack) => Left(not_success.toString)
      case Failure(ex) => Left(ex.getMessage)
    }
  }

  def isReady: Future[Boolean] = (hash ? Ready).mapTo[Boolean]

  def get(key: String): Either[Err, V] = {
    val getF = (hash ? Get(key)).mapTo[Option[Value]]
    Try(Await.result(getF, d)) match {
      case Success(Some(v)) => Right(v.toArray)
      case Success(None) => Left(s"not_found key $key")
      case Failure(ex) => Left(ex.getMessage)
    }
  }

  def delete(key: String): Either[Err, V] = {
    get(key).fold(
      l => Left(l),
      r => Try(Await.result((hash ? Delete(key)).mapTo[Ack], d)) match {
        case Success(AckSuccess) => Right(r)
        case Success(not_success) => Left(not_success.toString)
        case Failure(ex) => Left(ex.getMessage)
      }
    )
  }

  def save():Future[String] = (hash ? Dump).mapTo[String]
  def load(path:String):Future[Any] = hash ? LoadDump(path)
  def iterate(path:String,foreach:(String,Array[Byte])=>Unit):Future[Any] = hash ? IterateDump(path,foreach)

  def close(): Unit = ()
}
