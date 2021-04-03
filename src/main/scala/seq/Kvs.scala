package kvs.seq

import zio.RLayer
import zio.blocking.Blocking
import zio.clock.Clock

object Kvs {
  val feed    : KvsFeed    .type = KvsFeed
  val circular: KvsCircular.type = KvsCircular
  val file    : KvsFile    .type = KvsFile
  val search  : KvsSearch  .type = KvsSearch

  val live: RLayer[ActorSystem with Dba with Clock with Blocking, Kvs] =
    KvsFeed.live ++ KvsCircular.live ++ KvsFile.live ++ KvsSearch.live
}
