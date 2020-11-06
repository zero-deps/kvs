package kvs.seq

import zio.ZLayer

object Kvs {
  val list: KvsList.type = KvsList
  val circular: KvsCircular.type = KvsCircular
  val file: KvsFile.type = KvsFile
  val search: KvsSearch.type = KvsSearch

  val live: ZLayer[ActorSystem with Dba, Throwable, Kvs] = KvsList.live ++ KvsCircular.live ++ KvsFile.live ++ KvsSearch.live
}
