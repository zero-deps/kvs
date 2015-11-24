package mws.kvs

import akka.actor.{ActorSystem, ExtensionKey, Extension, ExtendedActorSystem}
import com.typesafe.config.Config

/** Akka Extension to interact with KVS storage as built into Akka */
object KvsExt extends ExtensionKey[KvsU]{
  override def lookup = KvsExt
  override def createExtension(system: ExtendedActorSystem): KvsU = new KvsU(system)
}

object KvsU {
  class Cfg(cfg:Config){
    val kvs = cfg.getConfig("mws.kvs")
    val dba = kvs.getString("dba")
    val sch = kvs.getConfig("scheme")
  }
}

class KvsU(val system: ExtendedActorSystem) extends Extension {
  import KvsU._

  val cfg = new Cfg(system.settings.config)
//  val log = Logging(system, getClass.getName)

  val dba:KvsU = system.dynamicAccess.getObjectFor[KvsU](cfg.dba).get

  //:create_schema(node()) ok
  // create_tables(name, [att, info(record)]), add_table_index
  //:start ok
  //:stop ok
  //:destroy 
  //:join
  //:version
  //:dir 
  // info()-> processes, transactions, schema, etc.
  //:config
  // ---------
  //:containers
  //:create
  //:link
  //:add
  //:delete
  //:traversal
  //:entries
  //:put
  //:get
  //:all
  //:index
  //:next_id
  //:save_db
  //:load_db
  //:dump
  //-------
  //:id_sec
  //:last_disc  ^
  //:last_table ^
  //:update_cfg ^

  // store handlers
  // - same with modified data
}
