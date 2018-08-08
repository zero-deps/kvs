package leveldbjnr;

import jnr.ffi.Pointer;
import jnr.ffi.byref.PointerByReference;
import jnr.ffi.byref.NumberByReference;
import jnr.ffi.annotations.IgnoreError;
import jnr.ffi.annotations.In;
import jnr.ffi.annotations.Out;
import jnr.ffi.annotations.Delegate;

public interface LevelDBNative {
  @IgnoreError Pointer leveldb_open(@In Pointer options, @In String name, PointerByReference errptr);
  @IgnoreError void leveldb_close(@In Pointer levelDB);
  @IgnoreError Pointer leveldb_get(@In Pointer levelDB, @In Pointer options, @In byte[] key, @In long keylen, @Out NumberByReference vallen, PointerByReference errptr);
  @IgnoreError Pointer leveldb_get(@In Pointer levelDB, @In Pointer options, @In byte[] key, @In int keylen, @Out NumberByReference vallen, PointerByReference errptr);
  @IgnoreError void leveldb_put(Pointer levelDB, Pointer options, byte[] key, long keylen, byte[] val, long vallen, PointerByReference errptr);
  @IgnoreError void leveldb_put(Pointer levelDB, Pointer options, byte[] key, int keylen, byte[] val, int vallen, PointerByReference errptr);
  @IgnoreError void leveldb_delete(Pointer levelDB, Pointer options, byte[] key, long keylen, PointerByReference errptr);
  @IgnoreError void leveldb_delete(Pointer levelDB, Pointer options, byte[] key, int keylen, PointerByReference errptr);
  @IgnoreError void leveldb_write(Pointer levelDB, Pointer options, Pointer batch, PointerByReference errptr);
  @IgnoreError String leveldb_property_value(Pointer levelDB, String propname);
  @IgnoreError void leveldb_approximate_sizes(Pointer levelDB, int num_ranges, Pointer range_start_key, long[] range_start_key_len, Pointer range_limit_key, long[] range_limit_key_len, Pointer sizes);
  @IgnoreError void leveldb_approximate_sizes(Pointer levelDB, int num_ranges, Pointer range_start_key, int[] range_start_key_len, Pointer range_limit_key, int[] range_limit_key_len, Pointer sizes);
  @IgnoreError void leveldb_compact_range(Pointer levelDB, byte[] start_key, long start_key_len, byte[] limit_key, long limit_key_len);
  @IgnoreError void leveldb_compact_range(Pointer levelDB, byte[] start_key, int start_key_len, byte[] limit_key, int limit_key_len);
  @IgnoreError void leveldb_destroy_db(Pointer options, String name, PointerByReference errptr);
  @IgnoreError void leveldb_repair_db(Pointer options, String name, PointerByReference errptr);
  @IgnoreError int leveldb_major_version();
  @IgnoreError int leveldb_minor_version();

  @IgnoreError Pointer leveldb_options_create();
  @IgnoreError void leveldb_options_destroy(Pointer options);
  @IgnoreError void leveldb_options_set_comparator(Pointer options, Pointer comparator);
  @IgnoreError void leveldb_options_set_filter_policy(Pointer options, Pointer filterPolicy);
  @IgnoreError void leveldb_options_set_create_if_missing(Pointer options, byte value);
  @IgnoreError void leveldb_options_set_error_if_exists(Pointer options, byte value);
  @IgnoreError void leveldb_options_set_paranoid_checks(Pointer options, byte value);
  @IgnoreError void leveldb_options_set_env(Pointer options, Pointer env);
  @IgnoreError void leveldb_options_set_info_log(Pointer options, Pointer logger);
  @IgnoreError void leveldb_options_set_write_buffer_size(Pointer options, long writeBufferSize);
  @IgnoreError void leveldb_options_set_write_buffer_size(Pointer options, int writeBufferSize);
  @IgnoreError void leveldb_options_set_max_open_files(Pointer options, int maxOpenFiles);
  @IgnoreError void leveldb_options_set_cache(Pointer options, Pointer cache);
  @IgnoreError void leveldb_options_set_block_size(Pointer options, long blockSize);
  @IgnoreError void leveldb_options_set_block_size(Pointer options, int blockSize);
  @IgnoreError void leveldb_options_set_block_restart_interval(Pointer options, int blockRestartInterval);
  @IgnoreError void leveldb_options_set_compression(Pointer options, int compressionType);

  @IgnoreError Pointer leveldb_readoptions_create();
  @IgnoreError void leveldb_readoptions_destroy(Pointer readOptions);
  @IgnoreError void leveldb_readoptions_set_verify_checksums(Pointer readOptions, byte value);
  @IgnoreError void leveldb_readoptions_set_fill_cache(Pointer readOptions, byte value);
  @IgnoreError void leveldb_readoptions_set_snapshot(Pointer readOptions, Pointer snapshot);

  @IgnoreError Pointer leveldb_writeoptions_create();
  @IgnoreError void leveldb_writeoptions_destroy(Pointer writeOptions);
  @IgnoreError void leveldb_writeoptions_set_sync(Pointer writeOptions, byte value);

  @IgnoreError Pointer leveldb_cache_create_lru(long capacity);
  @IgnoreError Pointer leveldb_cache_create_lru(int capacity);
  @IgnoreError void leveldb_cache_destroy(Pointer cache);

  // @IgnoreError Pointer leveldb_comparator_create(Pointer state, DestructorFunc destructorFunc, CompareFunc compareFunc);
  // @IgnoreError void leveldb_comparator_destroy(Pointer comparator);

  // public interface CompareFunc {
  //   @Delegate int invoke(Pointer pointer, byte[] a, long alen, byte[] b, long blen);
  // }

  @IgnoreError Pointer leveldb_create_default_env();
  @IgnoreError void leveldb_env_destroy(Pointer cache);

  // @IgnoreError Pointer leveldb_filterpolicy_create(Pointer state, DestructorFunc destructorFunc, CreateFilterFunc createFilterFunc, KeyMayMatchFunc keyMayMatchFunc, NameFunc nameFunc);
  @IgnoreError void leveldb_filterpolicy_destroy(Pointer filterPolicy);
  @IgnoreError Pointer leveldb_filterpolicy_create_bloom(int bits_per_key);

  // public interface CreateFilterFunc {
  //   @Delegate void invoke(Pointer pointer, PointerByReference key_array, PointerByReference key_length_array, int num_keys, PointerByReference filter_length);
  // }

  // public interface KeyMayMatchFunc {
  //   @Delegate void invoke(Pointer pointer, byte[] key, long length, byte[] filter, long filter_length);
  // }

  // public interface NameFunc {
  //   @Delegate byte[] invoke(Pointer pointer);
  // }

  @IgnoreError Pointer leveldb_create_iterator(Pointer levelDB, Pointer options);
  @IgnoreError Pointer leveldb_iter_destroy(Pointer iterator);
  @IgnoreError byte leveldb_iter_valid(Pointer iterator);
  @IgnoreError void leveldb_iter_seek_to_first(Pointer iterator);
  @IgnoreError void leveldb_iter_seek_to_last(Pointer iterator);
  @IgnoreError void leveldb_iter_seek(Pointer iterator, byte[] k, long klen);
  @IgnoreError void leveldb_iter_seek(Pointer iterator, byte[] k, int klen);
  @IgnoreError void leveldb_iter_next(Pointer iterator);
  @IgnoreError void leveldb_iter_prev(Pointer iterator);
  @IgnoreError Pointer leveldb_iter_key(Pointer iterator, NumberByReference klen);
  @IgnoreError Pointer leveldb_iter_value(Pointer iterator, NumberByReference vlen);
  @IgnoreError void leveldb_iter_get_error(Pointer iterator, PointerByReference errptr);

  @IgnoreError Pointer leveldb_create_snapshot(Pointer levelDB);
  @IgnoreError void leveldb_release_snapshot(Pointer levelDB, Pointer snapshot);

  @IgnoreError Pointer leveldb_writebatch_create();
  @IgnoreError void leveldb_writebatch_destroy(Pointer writeBatch);
  @IgnoreError void leveldb_writebatch_clear(Pointer writeBatch);
  @IgnoreError void leveldb_writebatch_put(Pointer writeBatch, byte[] key, long klen, byte[] val, long vlen);
  @IgnoreError void leveldb_writebatch_put(Pointer writeBatch, byte[] key, int klen, byte[] val, int vlen);
  @IgnoreError void leveldb_writebatch_delete(Pointer writeBatch, byte[] key, long klen);
  @IgnoreError void leveldb_writebatch_delete(Pointer writeBatch, byte[] key, int klen);
  // @IgnoreError void leveldb_writebatch_iterate(Pointer writeBatch, Pointer state, PutFunc putFunc, DeleteFunc deleteFunc);

  // public interface PutFunc {
  //   @Delegate void invoke(Pointer pointer, byte[] k, long klen, byte[] value, long vlen);
  // }

  // public interface DeleteFunc {
  //   @Delegate void invoke(Pointer pointer, byte[] k, long klen);
  // }

  @IgnoreError void leveldb_free(Pointer pointer);

  // public interface DestructorFunc {
  //   @Delegate void invoke(Pointer pointer);
  // }
}
