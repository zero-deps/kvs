package mws.kvs

import store._
import handle._

import akka.actor.{ExtensionKey, Extension, ExtendedActorSystem}
import com.typesafe.config.Config

import scala.concurrent.Future

/** 
 * Akka Extension to interact with KVS storage as built into Akka.
 */
object Kvs extends ExtensionKey[Kvs] {
  override def lookup = Kvs
  override def createExtension(system: ExtendedActorSystem): Kvs = new Kvs(system)
}
class Kvs(system: ExtendedActorSystem) extends Extension {
  import scala.collection.JavaConverters._

  val c = system.settings.config
  val kvsCfg = c.getConfig("kvs")
  val store = kvsCfg.getString("store")
  val feeds = kvsCfg.getStringList("feeds").asScala

  import scala.collection._

  implicit val dba:Dba = system.dynamicAccess.createInstanceFor[Dba](store,
    immutable.Seq(classOf[ExtendedActorSystem]-> system)).get

  //todo: create system feeds

  import scala.pickling._, Defaults._,binary._

  //todo: call directly for now. should be managed by feed server for sequential consistency.
  def put[H:Handler](el:H):Either[Err,H] = implicitly[Handler[H]].put(el)
  def get[H: Handler](k:String):Either[Err,H] = implicitly[Handler[H]].get(k)
  def delete[H:Handler](key: String): Either[Err,H] = implicitly[Handler[H]].delete(key)
  def add[H:Handler](el:H):Either[Err,H] = implicitly[Handler[H]].add(el)
  def remove[H:Handler](el:H):Either[Err,H] = implicitly[Handler[H]].remove(el)
  def entries[H:Handler](fid:String):Either[Err,List[H]] = entries(fid,None,None)
  def entries[H:Handler](fid:String,from:Option[H],count:Option[Int]):Either[Err,List[H]] = implicitly[Handler[H]].entries(fid,from,count)

  def isReady: Future[Boolean] = dba.isReady
  def close:Unit = dba.close
  def config:Config = kvsCfg
}
