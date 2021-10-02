package kvs.seq

import zio.RLayer

object Kvs:
  val feed: KvsFeed.type = KvsFeed

  val live: RLayer[ActorSystem with Dba, Kvs] =
    KvsFeed.live
