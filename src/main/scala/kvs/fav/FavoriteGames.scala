package mws.kvs
package favorite

import scala.collection.mutable.ArrayBuffer
import scala.language.postfixOps

/*object FavoriteGames {
  def apply[G](games: ArrayBuffer[G] = ArrayBuffer()): FavoriteGames[G] =
    new FavoriteGames(games)
}

class FavoriteGames[G](private val games: ArrayBuffer[G]) extends Serializable {
  def allGames = {
//    var i = 0
//    val newGames = games map { game => 
//      i = i + 1
//      game.setPriority(i)
//    }
    games.toList
  }

  def addGame(game: G): String = addGame(game, priority = 0)

  private def addGame(game: G, priority: Int): String = {
    games.find(_ == game) match {
      case Some(game) => game.id
      case None =>
        val gameWithID = if (game.id.isEmpty) game.setId(uuid) else game

        if (0 < priority && priority <= games.size)
          games.insert(priority - 1, gameWithID)
        else
          games.append(gameWithID)

        gameWithID.id
    }
  }
*/
  /** Returns true if game has been deleted */
/*  def removeGame(id: String): Boolean = {
    val oldSize = games.size
    games filter (_.id == id) foreach (games -= _)
    oldSize != games.size
  }

  def setPriority(id: String, priority: Int): Option[Game] = {
    if (priority <= games.size && games(priority - 1).id == id)
      Option(games(priority - 1).setPriority(priority))
    else
      getGameById(id) match {
        case Some(game) =>
          removeGame(id)
          addGame(game, priority)
          getGameById(id)
        case None => None
      }
  }

  def getGameById(id: String): Option[Game] = {
    var i = 0
    games foreach { game: Game =>
      i = i + 1
      if (game.id == id) return Option(game.setPriority(i))
    }
    None
  }

  private def uuid = java.util.UUID.randomUUID.toString
}*/
