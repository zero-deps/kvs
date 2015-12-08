package mws.kvs.importer

object ImporterService {
  object StartImport

  sealed trait Internal
  case object ImportFromFileRequest

  case class PutGames(games: List[Map[String, String]], ctx: String) extends Internal
  case class PutGamesToKvs(games: List[Map[String, String]], ctx: String) extends Internal
}
