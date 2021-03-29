package leveldbjnr

import java.io.File
import java.nio.file.{Files, StandardCopyOption}
import jnr.ffi.byref.{NumberByReference, PointerByReference}
import jnr.ffi.{LibraryLoader, LibraryOption, Pointer, TypeAlias}
import scala.util.{Try}

object LevelDb {
  private def copyLib(name: String): Either[Throwable, Long] = {
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
  val lib = LibraryLoader.create(classOf[Api]).option(LibraryOption.IgnoreError, null).failImmediately().load("leveldb").nn

  private[leveldbjnr] def checkError(error: PointerByReference): Either[Throwable, Unit] = {
    val str = error.getValue
    val x = if (str != null) Left(new Exception(str.getString(0)))
    else Right(())
    lib.leveldb_free(str)
    x
  }

  def open(path: String): Either[Throwable, LevelDb] = {
    val opts = lib.leveldb_options_create()
    lib.leveldb_options_set_create_if_missing(opts, 1)
    lib.leveldb_options_set_write_buffer_size(opts, 200*1024*1024)
    lib.leveldb_options_set_max_open_files(opts, 2500)
    lib.leveldb_options_set_block_size(opts, 64*1024)
    val filterpolicy = lib.leveldb_filterpolicy_create_bloom(10)
    lib.leveldb_options_set_filter_policy(opts, filterpolicy)
    val cache = lib.leveldb_cache_create_lru(500*1024*1024)
    lib.leveldb_options_set_cache(opts, cache)
    val error = new PointerByReference
    val leveldb = lib.leveldb_open(opts, path, error)
    lib.leveldb_options_destroy(opts)
    // lib.leveldb_cache_destroy(cache)
    // lib.leveldb_filterpolicy_destroy(filterpolicy)
    checkError(error).map(_ => LevelDb(leveldb))
  }

  def destroy(path: String): Either[Throwable, Unit] = {
    val opts = lib.leveldb_options_create()
    val error = new PointerByReference
    lib.leveldb_destroy_db(opts, path, error)
    lib.leveldb_options_destroy(opts)
    checkError(error)
  }

  def version: (Int, Int) = {
    (lib.leveldb_major_version(), lib.leveldb_minor_version())
  }
}

case class LevelDb(leveldb: Pointer) {
  import LevelDb.{lib, checkError}

  def get(key: Array[Byte], readOptions: ReadOpts): Either[Throwable, Option[Array[Byte]]] = {
    val resultLengthPointer = new NumberByReference(TypeAlias.size_t)
    val error = new PointerByReference
    val result = Option(lib.leveldb_get(leveldb, readOptions.pointer, key, key.length.toLong, resultLengthPointer, error))
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

  def put(key: Array[Byte], value: Array[Byte], writeOptions: WriteOpts): Either[Throwable, Unit] = {
    val error = new PointerByReference
    lib.leveldb_put(leveldb, writeOptions.pointer, key, key.length.toLong, value, value.length.toLong, error)
    checkError(error)
  }

  def write(writeBatch: WriteBatch, writeOptions: WriteOpts): Either[Throwable, Unit] = {
    val error = new PointerByReference
    lib.leveldb_write(leveldb, writeOptions.pointer, writeBatch.pointer, error)
    checkError(error)
  }

  def delete(key: Array[Byte], writeOptions: WriteOpts): Either[Throwable, Unit] = {
    val error = new PointerByReference
    lib.leveldb_delete(leveldb, writeOptions.pointer, key, key.length.toLong, error)
    checkError(error)
  }

  def iter(): Iter = {
    new Iter(leveldb)
  }

  def compact(): Unit = {
    lib.leveldb_compact_range(leveldb, null, 0L, null, 0L)
  }

  def close(): Unit = {
    lib.leveldb_close(leveldb)
  }
}

class Iter(leveldb: Pointer) {
  import LevelDb.lib

  val p = lib.leveldb_create_iterator(leveldb, new ReadOpts().pointer)

  def seek(key: Array[Byte]): Unit = {
    lib.leveldb_iter_seek(p, key, key.length.toLong)
  }

  def seek_to_first(): Unit = {
    lib.leveldb_iter_seek_to_first(p)
  }

  def valid(): Boolean = {
    lib.leveldb_iter_valid(p) != 0.toByte
  }

  def next(): Unit = {
    lib.leveldb_iter_next(p)
  }

  def key(): Array[Byte] = {
    val resultLengthPointer = new NumberByReference(TypeAlias.size_t)
    val resultPointer = lib.leveldb_iter_key(p, resultLengthPointer)
    val resultLength = resultLengthPointer.intValue
    val resultAsByteArray = new Array[Byte](resultLength)
    resultPointer.get(0, resultAsByteArray, 0, resultLength)
    resultAsByteArray
  }

  def close(): Unit = {
    val _ = lib.leveldb_iter_destroy(p)
  }
}

case class WriteOpts(sync: Boolean = false) {
  import LevelDb.lib

  private[leveldbjnr] val pointer: Pointer = lib.leveldb_writeoptions_create()

  lib.leveldb_writeoptions_set_sync(pointer, if (sync) 1 else 0)

  def close(): Unit = {
    lib.leveldb_writeoptions_destroy(pointer)
  }
}

case class ReadOpts(verifyChecksum: Boolean = false, fillCache: Boolean = true) {
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
    lib.leveldb_writebatch_put(pointer, key, key.length.toLong, value, value.length.toLong)
  }

  def delete(key: Array[Byte]): Unit = {
    lib.leveldb_writebatch_delete(pointer, key, key.length.toLong)
  }

  def clear(): Unit = {
    lib.leveldb_writebatch_clear(pointer)
  }

  def close(): Unit = {
    lib.leveldb_writebatch_destroy(pointer)
  }
}

given nullCanEqual[A]: CanEqual[A, A | Null] = CanEqual.derived