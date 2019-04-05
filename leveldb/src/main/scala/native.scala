package leveldbjnr

import jnr.ffi.Pointer
import jnr.ffi.byref.{PointerByReference, NumberByReference}
import jnr.ffi.annotations.{In, Out}

trait Api {
  def leveldb_open(@In options: Pointer,
                   @In name: String,
                   errptr: PointerByReference): Pointer

  def leveldb_close(levelDB: Pointer): Unit

  def leveldb_get(levelDB: Pointer,
                  @In options: Pointer,
                  @In key: Array[Byte],
                  @In keylen: Long,
                  @Out vallen: NumberByReference,
                  errptr: PointerByReference): Pointer

  def leveldb_put(levelDB: Pointer,
                  @In options: Pointer,
                  @In key: Array[Byte],
                  @In keylen: Long,
                  @In `val`: Array[Byte],
                  @In vallen: Long,
                  errptr: PointerByReference): Unit

  def leveldb_delete(levelDB: Pointer,
                     @In options: Pointer,
                     @In key: Array[Byte],
                     @In keylen: Long,
                     errptr: PointerByReference): Unit

  def leveldb_write(levelDB: Pointer,
                    @In options: Pointer,
                    @In batch: Pointer,
                    errptr: PointerByReference): Unit

  def leveldb_property_value(levelDB: Pointer, @In propname: String): String

  def leveldb_compact_range(levelDB: Pointer,
                            @In start_key: Array[Byte],
                            @In start_key_len: Long,
                            @In limit_key: Array[Byte],
                            @In limit_key_len: Long): Unit

  def leveldb_destroy_db(@In options: Pointer,
                         @In name: String,
                         errptr: PointerByReference): Unit

  def leveldb_repair_db(@In options: Pointer,
                        @In name: String,
                        errptr: PointerByReference): Unit

  def leveldb_major_version(): Int

  def leveldb_minor_version(): Int

  def leveldb_options_create(): Pointer

  def leveldb_options_destroy(options: Pointer): Unit

  def leveldb_options_set_comparator(options: Pointer,
                                     @In comparator: Pointer): Unit

  def leveldb_options_set_filter_policy(options: Pointer,
                                        @In filterPolicy: Pointer): Unit

  def leveldb_options_set_create_if_missing(options: Pointer,
                                            @In value: Byte): Unit

  def leveldb_options_set_error_if_exists(options: Pointer,
                                          @In value: Byte): Unit

  def leveldb_options_set_paranoid_checks(options: Pointer,
                                          @In value: Byte): Unit

  def leveldb_options_set_env(options: Pointer, @In env: Pointer): Unit

  def leveldb_options_set_info_log(options: Pointer, @In logger: Pointer): Unit

  def leveldb_options_set_write_buffer_size(options: Pointer,
                                            @In writeBufferSize: Long): Unit

  def leveldb_options_set_max_open_files(options: Pointer,
                                         @In maxOpenFiles: Int): Unit

  def leveldb_options_set_cache(options: Pointer, @In cache: Pointer): Unit

  def leveldb_options_set_block_size(options: Pointer,
                                     @In blockSize: Long): Unit

  def leveldb_options_set_block_restart_interval(
      options: Pointer,
      @In blockRestartInterval: Int): Unit

  def leveldb_options_set_compression(options: Pointer,
                                      @In compressionType: Int): Unit

  def leveldb_readoptions_create(): Pointer

  def leveldb_readoptions_destroy(readOptions: Pointer): Unit

  def leveldb_readoptions_set_verify_checksums(readOptions: Pointer,
                                               @In value: Byte): Unit

  def leveldb_readoptions_set_fill_cache(readOptions: Pointer,
                                         @In value: Byte): Unit

  def leveldb_readoptions_set_snapshot(readOptions: Pointer,
                                       @In snapshot: Pointer): Unit

  def leveldb_writeoptions_create(): Pointer

  def leveldb_writeoptions_destroy(writeOptions: Pointer): Unit

  def leveldb_writeoptions_set_sync(writeOptions: Pointer,
                                    @In value: Byte): Unit

  def leveldb_cache_create_lru(@In capacity: Long): Pointer

  def leveldb_cache_destroy(cache: Pointer): Unit

  def leveldb_create_default_env(): Pointer

  def leveldb_env_destroy(env: Pointer): Unit

  def leveldb_filterpolicy_destroy(filterPolicy: Pointer): Unit

  def leveldb_filterpolicy_create_bloom(@In bits_per_key: Int): Pointer

  def leveldb_create_iterator(levelDB: Pointer, @In options: Pointer): Pointer

  def leveldb_iter_destroy(iterator: Pointer): Pointer

  def leveldb_iter_valid(@In iterator: Pointer): Byte

  def leveldb_iter_seek_to_first(iterator: Pointer): Unit

  def leveldb_iter_seek_to_last(iterator: Pointer): Unit

  def leveldb_iter_seek(iterator: Pointer,
                        @In k: Array[Byte],
                        @In klen: Long): Unit

  def leveldb_iter_next(iterator: Pointer): Unit

  def leveldb_iter_prev(iterator: Pointer): Unit

  def leveldb_iter_key(@In iterator: Pointer,
                       @Out klen: NumberByReference): Pointer

  def leveldb_iter_value(@In iterator: Pointer,
                         @Out vlen: NumberByReference): Pointer

  def leveldb_iter_get_error(@In iterator: Pointer,
                             errptr: PointerByReference): Unit

  def leveldb_create_snapshot(levelDB: Pointer): Pointer

  def leveldb_release_snapshot(levelDB: Pointer, @In snapshot: Pointer): Unit

  def leveldb_writebatch_create(): Pointer

  def leveldb_writebatch_destroy(writeBatch: Pointer): Unit

  def leveldb_writebatch_clear(writeBatch: Pointer): Unit

  def leveldb_writebatch_put(writeBatch: Pointer,
                             @In key: Array[Byte],
                             @In klen: Long,
                             @In `val`: Array[Byte],
                             @In vlen: Long): Unit

  def leveldb_writebatch_delete(writeBatch: Pointer,
                                @In key: Array[Byte],
                                @In klen: Long): Unit

  def leveldb_free(pointer: Pointer): Unit

}
