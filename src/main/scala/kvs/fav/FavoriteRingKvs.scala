package mws.kvs
package favorite

import akka.actor.{ Props, ActorRef }
import com.typesafe.config.Config
import mws.kvs.RingKvs

object FavoriteRingKvs {
//  def props(importerConfig: Config,
//    importerKvs: ActorRef,
//    schemaName: String) = Props(new FavoriteRingKvs(importerKvs, importerConfig, schemaName))
}

abstract class FavoriteRingKvs(val importerKvs: ActorRef, val importerConfig: Config, val schemaName: String) {//extends FavoriteKvsService with RingKvs {
//  val maxNumberOfGames = {
//    val config = context.system.settings.config
//    if (config.hasPath("favorite.kvs.maxNumberOfGames")) {
//      config.getInt("favorite.kvs.maxNumberOfGames")
//    } else {
//      100
//    }
//  }
}
