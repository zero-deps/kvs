package mws.kvs
package favorite

import akka.actor.{Actor,ActorLogging,ActorRef,Props}
import akka.pattern.ask
import akka.util.Timeout
import au.com.bytecode.opencsv.CSVReader
//import com.fasterxml.jackson.core.`type`.TypeReference
//import com.fasterxml.jackson.databind.ObjectMapper
//import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.config.Config
import java.io.{ InputStream, InputStreamReader }
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.{ implicitConversions, postfixOps }
import scala.util.{ Failure, Success, Try }

import mws.kvs.Kvs
//import mws.kvs.importer._

//import com.playtech.mws.api.{ Context, Game }

object FavHandler {
  trait FavContext[C] {
    def sender:ActorRef
    def ctx:C
  }

  case class AddGame(game: Game, sender: ActorRef, ctx: Context) extends FavContext[Context]
  case class GameAdded(game: Game, sender: ActorRef, ctx: Context) extends FavContext[Context]


/*  case class GetFavoritesRequest[C](originalSender: ActorRef, ctx: C) extends FavoritesRequest[C]

  case class GetFavoritesResponse[C,G](games: FavoriteGames[G], originalSender: ActorRef, ctx: C) extends FavoritesResponse[C]

  case class PutFavoriteRequest[G,C](game: G, originalSender: ActorRef, ctx: C) extends FavoritesRequest[C]
  case class PutFavoriteResponse[G,C](game: G, originalSender: ActorRef, ctx: C) extends FavoritesResponse[C]

  case class RemoveFavorites[C](originalSender: ActorRef, ctx: C) extends FavoritesRequest[C]

  case class RemoveFavorite[C](gameId: String, originalSender: ActorRef, ctx: C) extends FavoritesRequest[C]
  case class RemoveFavoriteResponse[C](gameId: String, originalSender: ActorRef, ctx: C) extends FavoritesResponse[C]

  case class ChangePriorityRequest[C](gameId: String, priority: Int, originalSender: ActorRef, ctx: C) extends FavoritesRequest[C]
  case class ChangePriorityResponse[G,C](gameId: String, game: Option[G], originalSender: ActorRef, ctx: C) extends FavoritesResponse[C]

  case class FavoriteErrorResponse[C](message: String, originalSender: ActorRef, ctx: C) extends FavoritesResponse[C]

  val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
*/
  def props():Props = Props(classOf[FavHandler])
}

trait FavHandler extends Actor with ActorLogging { kvs: Kvs =>
  import FavHandler._

  def receive = {
    case AddGame(game,sdr,ctx) =>
      log.info("add $game $ctx")
      sender ! "ok"
    case GameAdded(game,sdr,ctx) =>
      log.info("game added $ctx")
      sender ! "ok"
    case _ => 
      log.info("not fav handler related")
      sender ! "ok"
  }


//  def allGames(user:String):(String,FavoriteGames[Game]) = {
//    kvs.get(user,classOf[String])
//  }

//  def getAllFavoriteGamesForUser[G](userId: String): Future[(String, FavoriteGames[G])] =
//    get(userId, classOf[String]).map {
//      case Some(json) =>
//        val xs: List[Map[String, String]] =
//          mapper.readValue(json, new TypeReference[List[Map[String, String]]] {})
//        val games = xs map (G apply _)
//        (userId, FavoriteGames(ArrayBuffer(games: _*)))
//      case None =>
//        (userId, FavoriteGames[G],empty)
//    }

//  import scala.collection.JavaConversions._
//  import scala.concurrent.ExecutionContext.Implicits.global

/*  val maxNumberOfGames: Int
  val importerConfig: Config
  val importerCsvSources = importerConfig.getStringList("csv.paths")
  val importerCsvDelimiter = importerConfig.getString("csv.delimiter")(0)
  val importerKvs: ActorRef

  implicit val timeout: Timeout = 1 second

  def receive = {
    case GetFavoritesRequest(originalSender, ctx) =>
      val client = sender()
      getAllFavoriteGamesForUser[G](ctx.user.toString) onSuccess {
        case (_, game) => client ! GetFavoritesResponse(game, originalSender, ctx)
      }

    case PutFavoriteRequest(game, originalSender, ctx) => {
      val userId: String = ctx.user.toString
      val sender = context.sender()
      getAllFavoriteGamesForUser[G](userId) onSuccess {
        case (_, games) =>
          val gameId = games.addGame(game)
          val newGame = games.getGameById(gameId).get
          if (games.allGames.size <= maxNumberOfGames) {
            putAllFavoriteGamesForUser(userId, games) onComplete {
              case Success(_) =>
                sender ! PutFavoriteResponse(newGame, originalSender, ctx)
              case Failure(ex) =>
                log.error(ex.getMessage, ex)
                sender ! FavoriteErrorResponse(ex.getMessage, originalSender, ctx)
            }
          } else {
            sender ! FavoriteErrorResponse(s"The maximum number of games in favorites is ${maxNumberOfGames}", originalSender, ctx)
          }
      }
    }

    case RemoveFavorites(_, ctx: C) =>
      removeAllFavoriteGamesForUser(ctx.user.toString)

    case RemoveFavorite(gameId, originalSender, ctx) => {
      val sender: ActorRef = context.sender
      getAllFavoriteGamesForUser[G](ctx.user.toString) onSuccess {
        case (_, games) =>
          if (games.removeGame(gameId)) {
            putAllFavoriteGamesForUser(ctx.user.toString, games)
            sender ! RemoveFavoriteResponse(gameId, originalSender, ctx)
          } else {
            sender ! FavoriteErrorResponse(s"Can't remove game with ID ${gameId}!", originalSender, ctx)
          }
      }
    }

    case ChangePriorityRequest(gameId, priority, originalSender, ctx) => {
      val sender = context.sender()
      changePriority(ctx.user.toString, gameId, priority) onComplete {
        case Success(game) => sender ! ChangePriorityResponse(gameId, game, originalSender, ctx)
        case Failure(ex) => sender ! FavoriteErrorResponse(s"Can't change priority for game with ID ${gameId}!", originalSender, ctx)
      }
    }

    case PutGamesToKvs(games, userId) => {
      val json = mapper.writeValueAsString(games)
      kvs.put(userId, json)
    }

    case ImporterService.ImportFromFileRequest =>
      log.info("Starting import of favorites")
      val sender = context.sender
      importerCsvSources foreach { filePath =>
        getFileStream(filePath) match {
          case Some(_) =>
            importerKvs ? ImporterKvs.Get(filePath) onSuccess {
              case ImporterKvs.GetAck(importTime, filePath) =>
                importTime match {
                  case Some(time) =>
                    log.info(s"Skipping $filePath because it was already imported at $time")
                  case None =>
                    val csvReader = getCsvReader(getFileStream(filePath).get)
                    Try({
                      val rows = csvReader.readAll.toList
                      rows match {
                        case header :: data =>
                          val newGames = data.map { item =>
                            val attrs = (header zip item).toMap
                            val userId = attrs.get("userId")
                            val game = Game(attrs - "userId")
                            userId -> game
                          }.collect {
                            case (Some(userId), game) => (userId, game)
                          }.foldLeft(Map.empty[String, List[G]]) {
                            case (acc, (userId, game)) =>
                              val userGames = game :: acc.getOrElse(userId, Nil)
                              acc + (userId -> userGames)
                          }
                          val fs = newGames.map(_._1).map(getAllFavoriteGamesForUser)
                          Future.sequence(fs) onSuccess {
                            case results =>
                              results.foreach {
                                case (userId, games) =>
                                  newGames.getOrElse(userId, Nil).foreach(games.addGame)
                                  putAllFavoriteGamesForUser(userId, games)
                              }
                              importerKvs ? ImporterKvs.Put(filePath) onSuccess {
                                case x @ ImporterKvs.PutAck(filePath) =>
                                  log.info(s"File $filePath has been imported")
                                  sender ! x
                              }
                          }
                        case _ =>
                          log.warning(s"Bad CSV file: $filePath")
                      }
                    }) match {
                      case Success(_) =>
                      case Failure(e) => log.error(e, e.getMessage)
                    }
                    Try(csvReader.close())
                }
            }
          case None =>
            log.warning(s"File $filePath doesn't exist")
        }
      }
  }

  def getAllFavoriteGamesForUser[G](userId: String): Future[(String, FavoriteGames[G])] =
    get(userId, classOf[String]).map {
      case Some(json) =>
        val xs: List[Map[String, String]] =
          mapper.readValue(json, new TypeReference[List[Map[String, String]]] {})
        val games = xs map (G apply _)
        (userId, FavoriteGames(ArrayBuffer(games: _*)))
      case None =>
        (userId, FavoriteGames[G],empty)
    }

  def putAllFavoriteGamesForUser(userId: String, favoriteGames: FavoriteGames) = {
    val games = favoriteGames.allGames map (_.attributes)
    if (games.size > maxNumberOfGames)
      kvs.put(userId, mapper.writeValueAsString(games.take(maxNumberOfGames)))
    else
      kvs.put(userId, mapper.writeValueAsString(games))
  }

  def removeAllFavoriteGamesForUser(userId: String): Unit =
    delete(userId)

  def changePriority(userId: String, gameId: String, priority: Int): Future[Option[G]] = {
    getAllFavoriteGamesForUser[G](userId) map {
      case (_, games) =>
        val modifiedGame = games.setPriority(gameId, priority)
        putAllFavoriteGamesForUser(userId, games)
        modifiedGame
    }
  }

  def getFileStream(filePath: String): Option[InputStream] = {
    Option(getClass.getClassLoader.getResourceAsStream(filePath)) match {
      case Some(x) => Some(x)
      case None => Option(getClass.getResourceAsStream(filePath))
    }
  }

  def getCsvReader(fileStream: InputStream): CSVReader =
    new CSVReader(new InputStreamReader(fileStream), importerCsvDelimiter)
*/
}
