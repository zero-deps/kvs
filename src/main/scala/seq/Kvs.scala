package kvs.seq

import zio.ZLayer

object Kvs {
  val feed    : KvsFeed    .type = KvsFeed
  val circular: KvsCircular.type = KvsCircular
  val file    : KvsFile    .type = KvsFile
  val search  : KvsSearch  .type = KvsSearch

  val live: ZLayer[ActorSystem with Dba, Throwable, Kvs] = KvsFeed.live ++ KvsCircular.live ++ KvsFile.live ++ KvsSearch.live
}
