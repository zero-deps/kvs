package mws.kvs.favorite

//import akka.actor.ActorRef
//import com.playtech.mws.api.AuthUser
//import com.typesafe.config.ConfigFactory
//import mws.core.ActorTest
//import mws.core.importer.{ImporterKvs, ImporterService}
//import mws.core.service.ContextFactory
//import mws.core.service.favorite.storage.FavoriteKvsService._
//import mws.core.service.favorite.storage._

/*object FavoriteKvsImportTest {
  val config = ConfigFactory.parseString("""
    favorite.kvs.importer.csv {
      paths = ["/test.csv", "/favorites.csv"]
      delimiter = ";"
    }
  """)
}

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class FavoriteKvsImportTest extends ActorTest(FavoriteKvsImportTest.config) {

  val user1 = AuthUser("player01", "playtech81001")
  val user2 = AuthUser("player02", "playtech81001")
  val user3 = AuthUser("YODA116", "playtech81003")

  def createService() = {
    val importerKvs = system.actorOf(FavoriteImporterInMemoryKvs.props())
    val importerConfig = system.settings.config.getConfig("favorite.kvs.importer")
    system.actorOf(FavoriteInMemoryKvs.props(importerKvs, importerConfig, 100))
  }

  test("service should import data from csv files") {
    val service = createService()
    service ! ImporterService.ImportFromFileRequest

    expectMsgClass(classOf[ImporterKvs.PutAck])
    expectMsgClass(classOf[ImporterKvs.PutAck])

    service ! GetFavoritesRequest(self, ContextFactory(user1))
    expectMsgPF() {
      case GetFavoritesResponse(favoriteGames: FavoriteGames, originalSender: ActorRef, ctx: Any) =>
        favoriteGames.allGames.length should be(2)
        favoriteGames.allGames.find(g =>
          g.getAttribute("gameCode") == "Gala Live" && g.getAttribute("gameType") == "VF_ROOM")
        favoriteGames.allGames.find(g =>
          g.getAttribute("gameCode") == "Gala Dead" && g.getAttribute("gameType") == "VF_ROOM")
    }

    service ! GetFavoritesRequest(self, ContextFactory(user2))
    expectMsgPF() {
      case GetFavoritesResponse(favoriteGames: FavoriteGames, originalSender: ActorRef, ctx: Any) =>
        favoriteGames.allGames.length should be(2)
        favoriteGames.allGames.find(g =>
          g.getAttribute("gameCode") == "Black Jack" && g.getAttribute("gameType") == "VF_GAME")
        favoriteGames.allGames.find(g =>
          g.getAttribute("gameCode") == "Iron" && g.getAttribute("gameType") == "VF_ROOM")
    }

    service ! GetFavoritesRequest(self, ContextFactory(user3))
    expectMsgPF() {
      case GetFavoritesResponse(favoriteGames: FavoriteGames, originalSender: ActorRef, ctx: Any) =>
        favoriteGames.allGames.length should be(1)
        favoriteGames.allGames.find(g =>
          g.getAttribute("gameCode") == "xmn50" && g.getAttribute("gameType") == "CASINO")
    }
  }
}
*/
