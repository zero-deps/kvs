package kvs.seq

import zio.RLayer

object Kvs {
  val feed    : KvsFeed    .type = KvsFeed
  val circular: KvsCircular.type = KvsCircular
  val file    : KvsFile    .type = KvsFile
  val search  : KvsSearch  .type = KvsSearch

  val live: RLayer[ActorSystem with Dba, Kvs] =
    KvsFeed.live ++ KvsCircular.live ++ KvsFile.live ++ KvsSearch.live
}
