package kvs.seq

import kvs.{FdKey, ElKey, EnKey, Res}
import zd.proto.api.{MessageCodec, encodeToBytes, decode}
import zd.proto.Bytes
import zio.{IO, Task, ZLayer, ZIO, UIO}
import zio.stream.Stream
import zio.akka.cluster.sharding.{Sharding, Entity}
import zio.macros.accessible

@accessible
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
