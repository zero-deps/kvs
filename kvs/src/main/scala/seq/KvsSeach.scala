package kvs.seq

import zio.*

object KvsSearch {
  trait Service {
  }
  
  val live: ZLayer[ActorSystem with Dba, Throwable, KvsSearch] = ZLayer.fromEffect {
    for {
      _ <- ZIO.unit
    } yield {
      new Service {
      }
    }
  }
}
