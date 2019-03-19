package mws.kvs

import leveldbjnr.{LevelDB, LevelDBOptions}

object LeveldbOps {
  def open(path: String): LevelDB = {
    val leveldbOptions = new LevelDBOptions() {
      val bloom = LevelDB.lib.leveldb_filterpolicy_create_bloom(10)
      LevelDB.lib.leveldb_options_set_filter_policy(options, bloom)
      val cache = LevelDB.lib.leveldb_cache_create_lru(500 * 1048576) // 100MB cache
      LevelDB.lib.leveldb_options_set_cache(options, cache)
      val writeBuffer = 200 * 1048576 // 10MB write buffer
      LevelDB.lib.leveldb_options_set_write_buffer_size(options, writeBuffer)
      // val fileSize = 50 * 1048576
      // LevelDB.lib.leveldb_options_set_max_file_size(options, fileSize)
    }
    leveldbOptions.setWriteBufferSize(200 * 1048576)
    leveldbOptions.setBlockSize(65536)
    leveldbOptions.setMaxOpenFiles(2500)
    leveldbOptions.setCreateIfMissing(true)
    val x = new LevelDB(path, leveldbOptions)
    leveldbOptions.close()
    x
  }
}
