package leveldbjnr

import java.io.File
import java.nio.file.{Files, StandardCopyOption}
import jnr.ffi.byref.{NumberByReference, PointerByReference}
import jnr.ffi.{LibraryLoader, LibraryOption, Pointer, TypeAlias}
import scala.util.{Try}

object LevelDb {
  private def copyLib(name: String): Throwable Either Long = {
    val is = classOf[LevelDb].getResourceAsStream(s"/lib/${name}")
    val dest = new File(s"./tmp/${name}")
    Try {
      dest.mkdirs()
      Files.copy(is, dest.toPath(), StandardCopyOption.REPLACE_EXISTING)
    }.toEither
  }

  copyLib("libleveldb.dylib")
  copyLib("libleveldb.so") 
  copyLib("leveldb.dll")
  sys.props += "java.library.path" -> "./tmp/"
  val lib = LibraryLoader.create(classOf[Api]).option(LibraryOption.IgnoreError, null).failImmediately().load("leveldb")

  def checkError(error: PointerByReference): Throwable Either Unit = {
    val pointerError = error.getValue
    if (pointerError != null) Left(new Exception(pointerError.getString(0)))
    else Right(())
  }

  def open(path: String): Throwable Either LevelDb = {
    for {
      opts <- Right(lib.leveldb_options_create())
      _ <- Right(lib.leveldb_options_set_create_if_missing(opts, 1))
      _ <- Right(lib.leveldb_options_set_error_if_exists(opts, 0))
      _ <- Right(lib.leveldb_options_set_paranoid_checks(opts, 0))
      _ <- Right(lib.leveldb_options_set_compression(opts, 1))
      _ <- Right(lib.leveldb_options_set_write_buffer_size(opts, 200*1024*1024))
      _ <- Right(lib.leveldb_options_set_max_open_files(opts, 2500))
      _ <- Right(lib.leveldb_options_set_block_size(opts, 65536))
      _ <- Right(lib.leveldb_options_set_block_restart_interval(opts, 16))
      filterpolicy <- Right(lib.leveldb_filterpolicy_create_bloom(10))
      _ <- Right(lib.leveldb_options_set_filter_policy(opts, filterpolicy))
      cache <- Right(lib.leveldb_cache_create_lru(500*1024*1024))
      _ <- Right(lib.leveldb_options_set_cache(opts, cache))
      _ <- Right(lib.leveldb_options_set_write_buffer_size(opts, 200*1024*1024))
      error <- Right(new PointerByReference)
      leveldb <- Right(lib.leveldb_open(opts, path, error))
      _ <- Right(lib.leveldb_options_destroy(opts))
      // _ <- Right(lib.leveldb_cache_destroy(cache))
      // _ <- Right(lib.leveldb_filterpolicy_destroy(filterpolicy))
      _ <- checkError(error)
    } yield LevelDb(leveldb)
  }

  def destroy(path: String): Throwable Either Unit = {
    val opts = lib.leveldb_options_create()
    val error = new PointerByReference
    lib.leveldb_destroy_db(opts, path, error)
    lib.leveldb_options_destroy(opts)
    checkError(error)
  }
}

case class LevelDb(leveldb: Pointer) {
  import LevelDb.{lib, checkError}

  def get(key: Array[Byte], readOptions: ReadOpts): Throwable Either Option[Array[Byte]] = {
    val resultLengthPointer = new NumberByReference(TypeAlias.size_t)
    val error = new PointerByReference
    val result = Option(lib.leveldb_get(leveldb, readOptions.pointer, key, key.length, resultLengthPointer, error))
    checkError(error).map{ _ =>
      result.map{ result =>
        val resultLength = resultLengthPointer.intValue
        val resultAsByteArray = new Array[Byte](resultLength)
        result.get(0, resultAsByteArray, 0, resultLength)
        lib.leveldb_free(result)
        resultAsByteArray
      }
    }
  }

  def put(key: Array[Byte], value: Array[Byte], writeOptions: WriteOpts): Throwable Either Unit = {
    val error = new PointerByReference
    lib.leveldb_put(leveldb, writeOptions.pointer, key, key.length, value, value.length, error)
    checkError(error)
  }

  def write(writeBatch: WriteBatch, writeOptions: WriteOpts): Throwable Either Unit = {
    val error = new PointerByReference
    lib.leveldb_write(leveldb, writeOptions.pointer, writeBatch.pointer, error)
    checkError(error)
  }

  def delete(key: Array[Byte], writeOptions: WriteOpts): Throwable Either Unit = {
    val error = new PointerByReference
    lib.leveldb_delete(leveldb, writeOptions.pointer, key, key.length, error);
    checkError(error)
  }

  def compact(): Unit = {
    lib.leveldb_compact_range(leveldb, null, 0L, null, 0L)
  }

  def close(): Unit = {
    lib.leveldb_close(leveldb)
  }
}

object WriteOpts {
  def apply(sync: Boolean = false): WriteOpts = new WriteOpts(sync)
}

case class WriteOpts(sync: Boolean) {
  import LevelDb.lib

  private[leveldbjnr] val pointer: Pointer = lib.leveldb_writeoptions_create()

  lib.leveldb_writeoptions_set_sync(pointer, if (sync) 1 else 0)

  def close(): Unit = {
    lib.leveldb_writeoptions_destroy(pointer)
  }
}

object ReadOpts {
  def apply(verifyChecksum: Boolean = false, fillCache: Boolean = true): ReadOpts = new ReadOpts(verifyChecksum, fillCache)
}

case class ReadOpts(verifyChecksum: Boolean, fillCache: Boolean) {
  import LevelDb.lib

  private[leveldbjnr] val pointer: Pointer = lib.leveldb_readoptions_create()

  lib.leveldb_readoptions_set_verify_checksums(pointer, if (verifyChecksum) 1 else 0)
  lib.leveldb_readoptions_set_fill_cache(pointer, if (fillCache) 1 else 0)

  def close(): Unit = {
    lib.leveldb_readoptions_destroy(pointer)
  }
}

case class WriteBatch() {
  import LevelDb.lib

  private[leveldbjnr] val pointer: Pointer = lib.leveldb_writebatch_create()

  def put(key: Array[Byte], value: Array[Byte]): Unit = {
    lib.leveldb_writebatch_put(pointer, key, key.length, value, value.length)
  }

  def delete(key: Array[Byte]): Unit = {
    lib.leveldb_writebatch_delete(pointer, key, key.length)
  }

  def clear(): Unit = {
    lib.leveldb_writebatch_clear(pointer)
  }

  def close(): Unit = {
    lib.leveldb_writebatch_destroy(pointer)
  }
}
