package leveldbjnr;

import jnr.ffi.Pointer;
import jnr.ffi.byref.PointerByReference;
import jnr.ffi.byref.NumberByReference;
import jnr.ffi.annotations.IgnoreError;
import jnr.ffi.annotations.In;
import jnr.ffi.annotations.Out;
import jnr.ffi.annotations.Delegate;

public interface LevelDBNative {
  Pointer leveldb_open(@In Pointer options, @In String name, PointerByReference errptr);
  void leveldb_close(Pointer levelDB);
  Pointer leveldb_get(Pointer levelDB, @In Pointer options, @In byte[] key, @In long keylen, @Out NumberByReference vallen, PointerByReference errptr);
  void leveldb_put(Pointer levelDB, @In Pointer options, @In byte[] key, @In long keylen, @In byte[] val, @In long vallen, PointerByReference errptr);
  void leveldb_delete(Pointer levelDB, @In Pointer options, @In byte[] key, @In long keylen, PointerByReference errptr);
  void leveldb_write(Pointer levelDB, @In Pointer options, @In Pointer batch, PointerByReference errptr);
  String leveldb_property_value(Pointer levelDB, @In String propname);
  void leveldb_compact_range(Pointer levelDB, @In byte[] start_key, @In long start_key_len, @In byte[] limit_key, @In long limit_key_len);
  void leveldb_destroy_db(@In Pointer options, @In String name, PointerByReference errptr);
  void leveldb_repair_db(@In Pointer options, @In String name, PointerByReference errptr);
  int leveldb_major_version();
  int leveldb_minor_version();

  Pointer leveldb_options_create();
  void leveldb_options_destroy(Pointer options);
  void leveldb_options_set_comparator(Pointer options, @In Pointer comparator);
  void leveldb_options_set_filter_policy(Pointer options, @In Pointer filterPolicy);
  void leveldb_options_set_create_if_missing(Pointer options, @In byte value);
  void leveldb_options_set_error_if_exists(Pointer options, @In byte value);
  void leveldb_options_set_paranoid_checks(Pointer options, @In byte value);
  void leveldb_options_set_env(Pointer options, @In Pointer env);
  void leveldb_options_set_info_log(Pointer options, @In Pointer logger);
  void leveldb_options_set_write_buffer_size(Pointer options, @In long writeBufferSize);
  void leveldb_options_set_max_open_files(Pointer options, @In int maxOpenFiles);
  void leveldb_options_set_cache(Pointer options, @In Pointer cache);
  void leveldb_options_set_block_size(Pointer options, @In long blockSize);
  void leveldb_options_set_block_restart_interval(Pointer options, @In int blockRestartInterval);
  void leveldb_options_set_compression(Pointer options, @In int compressionType);

  Pointer leveldb_readoptions_create();
  void leveldb_readoptions_destroy(Pointer readOptions);
  void leveldb_readoptions_set_verify_checksums(Pointer readOptions, @In byte value);
  void leveldb_readoptions_set_fill_cache(Pointer readOptions, @In byte value);
  void leveldb_readoptions_set_snapshot(Pointer readOptions, @In Pointer snapshot);

  Pointer leveldb_writeoptions_create();
  void leveldb_writeoptions_destroy(Pointer writeOptions);
  void leveldb_writeoptions_set_sync(Pointer writeOptions, @In byte value);

  Pointer leveldb_cache_create_lru(@In long capacity);
  void leveldb_cache_destroy(Pointer cache);

  Pointer leveldb_create_default_env();
  void leveldb_env_destroy(Pointer env);

  void leveldb_filterpolicy_destroy(Pointer filterPolicy);
  Pointer leveldb_filterpolicy_create_bloom(@In int bits_per_key);

  Pointer leveldb_create_iterator(Pointer levelDB, @In Pointer options);
  Pointer leveldb_iter_destroy(Pointer iterator);
  byte leveldb_iter_valid(@In Pointer iterator);
  void leveldb_iter_seek_to_first(Pointer iterator);
  void leveldb_iter_seek_to_last(Pointer iterator);
  void leveldb_iter_seek(Pointer iterator, @In byte[] k, @In long klen);
  void leveldb_iter_next(Pointer iterator);
  void leveldb_iter_prev(Pointer iterator);
  Pointer leveldb_iter_key(@In Pointer iterator, @Out NumberByReference klen);
  Pointer leveldb_iter_value(@In Pointer iterator, @Out NumberByReference vlen);
  void leveldb_iter_get_error(@In Pointer iterator, PointerByReference errptr);

  Pointer leveldb_create_snapshot(Pointer levelDB);
  void leveldb_release_snapshot(Pointer levelDB, @In Pointer snapshot);

  Pointer leveldb_writebatch_create();
  void leveldb_writebatch_destroy(Pointer writeBatch);
  void leveldb_writebatch_clear(Pointer writeBatch);
  void leveldb_writebatch_put(Pointer writeBatch, @In byte[] key, @In long klen, @In byte[] val, @In long vlen);
  void leveldb_writebatch_delete(Pointer writeBatch, @In byte[] key, @In long klen);

  void leveldb_free(Pointer pointer);
}
