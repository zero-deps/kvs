package kvs.seq

import zio.*

object KvsFile {
  trait Service {
  }

  val live: ZLayer[ActorSystem with Dba, Throwable, KvsFile] = ZLayer.fromEffect {
    for {
      _ <- ZIO.unit
    } yield {
      new Service {
      }
    }
  }
}
