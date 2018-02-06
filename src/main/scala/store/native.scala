package com.protonail.leveldb.jna

object native {
  def leveldb_filterpolicy_create_bloom(bits_per_key: Int): LevelDBNative.FilterPolicy =
    LevelDBNative.leveldb_filterpolicy_create_bloom(bits_per_key)

  def leveldb_options_set_filter_policy(options: LevelDBNative.Options, filterPolicy: LevelDBNative.FilterPolicy): Unit =
    LevelDBNative.leveldb_options_set_filter_policy(options, filterPolicy)

  def leveldb_options_set_cache(options: LevelDBNative.Options, cache: LevelDBNative.Cache): Unit =
    LevelDBNative.leveldb_options_set_cache(options, cache)

  def leveldb_cache_create_lru(capacity: Int): LevelDBNative.Cache =
    LevelDBNative.leveldb_cache_create_lru(capacity)
}
