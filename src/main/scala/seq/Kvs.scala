package kvs.seq

import zio.ZLayer

object Kvs {
  val list: KvsList.type = KvsList
  val array: KvsArray.type = KvsArray
  val file: KvsFile.type = KvsFile
  val search: KvsSearch.type = KvsSearch

  def live: ZLayer[ActorSystem with Dba, Throwable, Kvs] = KvsList.live ++ KvsArray.live ++ KvsFile.live ++ KvsSearch.live
}
