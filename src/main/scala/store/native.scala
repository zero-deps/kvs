package com.protonail.leveldb.jna

object native {
  def leveldb_filterpolicy_create_bloom(bits_per_key: Int): LevelDBNative.FilterPolicy =
    LevelDBNative.leveldb_filterpolicy_create_bloom(bits_per_key)

  def leveldb_options_set_filter_policy(options: LevelDBNative.Options, filterPolicy: LevelDBNative.FilterPolicy): Unit =
    LevelDBNative.leveldb_options_set_filter_policy(options, filterPolicy)
}
