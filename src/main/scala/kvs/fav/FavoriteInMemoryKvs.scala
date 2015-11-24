package mws.kvs
package favorite

import akka.actor.{ ActorRef, Props }
import com.typesafe.config.Config

object FavoriteInMemoryKvs {
  def props(importerKvs: ActorRef, importerConfig: Config, maxNumberOfGames: Int): Props =
    Props(classOf[FavoriteInMemoryKvs],importerKvs, importerConfig,  maxNumberOfGames)
}

abstract class FavoriteInMemoryKvs(val importerKvs: ActorRef,
  val importerConfig: Config,  val maxNumberOfGames: Int) //extends FavoriteKvsService
//  with InMemoryKvs