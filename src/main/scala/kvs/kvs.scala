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
  def add[H:Handler](el:H):Either[Err,H] = implicitly[Handler[H]].add(el)
  def remove[H:Handler](el:H):Either[Err,H] = implicitly[Handler[H]].remove(el)
  def entries[H:Handler]():Either[Err,Iterator[H]] = implicitly[Handler[H]].entries()

  def isReady: Future[Boolean] = dba.isReady
  def close:Unit = dba.close
  def config:Config = kvsCfg
}
