package leveldbjnr;

import jnr.ffi.Pointer;
import jnr.ffi.LibraryLoader;
import jnr.ffi.Runtime;
import jnr.ffi.TypeAlias;
import jnr.ffi.LibraryOption;
import jnr.ffi.byref.PointerByReference;
import jnr.ffi.byref.NumberByReference;
import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

public class LevelDB implements AutoCloseable {
  static void copyLib(String name) {
    try (InputStream is = LevelDB.class.getResourceAsStream("/lib/" + name)) {
      File dest = new File("./tmp/" + name);
      dest.mkdirs();
      Files.copy(is, dest.toPath(), StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  static {
    copyLib("libleveldb.dylib");
    copyLib("libleveldb.so"); 
    copyLib("leveldb.dll");
    System.setProperty("java.library.path", "./tmp/");
  }
  public static final LevelDBNative lib = LibraryLoader.create(LevelDBNative.class).option(LibraryOption.IgnoreError, new Object()).failImmediately().load("leveldb");
  public static final Runtime runtime = Runtime.getRuntime(lib);

  protected Pointer levelDB;

  public LevelDB(String levelDBDirectory, LevelDBOptions options) {
    options.checkOptionsOpen();

    PointerByReference error = new PointerByReference();
    levelDB = LevelDB.lib.leveldb_open(options.options, levelDBDirectory, error);
    LevelDB.checkError(error);
  }

  public void close() {
    checkDatabaseOpen();

    LevelDB.lib.leveldb_close(levelDB);
    levelDB = null;
  }

  public byte[] get(byte[] key, LevelDBReadOptions readOptions) {
    checkDatabaseOpen();
    readOptions.checkReadOptionsOpen();

    NumberByReference resultLengthPointer = new NumberByReference(TypeAlias.size_t);
    PointerByReference error = new PointerByReference();
    long keyLength = key != null ? key.length : 0;
    Pointer result = LevelDB.lib.leveldb_get(levelDB, readOptions.readOptions, key, keyLength, resultLengthPointer, error);
    checkError(error);
    int resultLength = resultLengthPointer.intValue();

    byte[] resultAsByteArray = null;
    if (result != null) {
      resultAsByteArray = new byte[resultLength];
      result.get(0, resultAsByteArray, 0, resultLength);
      LevelDB.lib.leveldb_free(result);
    }
    return resultAsByteArray;
  }

  public void put(byte[] key, byte[] value, LevelDBWriteOptions writeOptions) {
    checkDatabaseOpen();
    writeOptions.checkWriteOptionsOpen();

    PointerByReference error = new PointerByReference();
    long keyLength = key != null ? key.length : 0;
    long valueLength = value != null ? value.length : 0;
    LevelDB.lib.leveldb_put(levelDB, writeOptions.writeOptions, key, keyLength, value, valueLength, error);

    LevelDB.checkError(error);
  }

  public void write(LevelDBWriteBatch writeBatch, LevelDBWriteOptions writeOptions) {
    checkDatabaseOpen();
    writeOptions.checkWriteOptionsOpen();

    PointerByReference error = new PointerByReference();
    LevelDB.lib.leveldb_write(levelDB, writeOptions.writeOptions, writeBatch.writeBatch, error);
    LevelDB.checkError(error);
  }

  public void delete(byte[] key, LevelDBWriteOptions writeOptions) {
    checkDatabaseOpen();
    writeOptions.checkWriteOptionsOpen();

    PointerByReference error = new PointerByReference();
    long keyLength = key != null ? key.length : 0;
    LevelDB.lib.leveldb_delete(levelDB, writeOptions.writeOptions, key, keyLength, error);

    LevelDB.checkError(error);
  }

  public String property(String property) {
    checkDatabaseOpen();
    return LevelDB.lib.leveldb_property_value(levelDB, property);
  }

  public void compactRange(byte[] startKey, byte[] limitKey) {
    checkDatabaseOpen();

    long startKeyLength = startKey != null ? startKey.length : 0;
    long limitKeyLength = limitKey != null ? limitKey.length : 0;
    LevelDB.lib.leveldb_compact_range(levelDB, startKey, startKeyLength, limitKey, limitKeyLength);
  }

  public static void repair(String levelDBDirectory, LevelDBOptions options) {
    options.checkOptionsOpen();

    PointerByReference error = new PointerByReference();
    LevelDB.lib.leveldb_repair_db(options.options, levelDBDirectory, error);
    LevelDB.checkError(error);
  }

  public static void destroy(String levelDBDirectory, LevelDBOptions options) {
    options.checkOptionsOpen();

    PointerByReference error = new PointerByReference();
    LevelDB.lib.leveldb_destroy_db(options.options, levelDBDirectory, error);
    LevelDB.checkError(error);
  }

  public static int majorVersion() {
    return LevelDB.lib.leveldb_major_version();
  }

  public static int minorVersion() {
    return LevelDB.lib.leveldb_minor_version();
  }

  protected void checkDatabaseOpen() {
    if (levelDB == null) throw new LevelDBException("LevelDB database was closed.");
  }

  protected static void checkError(PointerByReference error) {
    Pointer pointerError = error.getValue();
    if (pointerError != null) {
      String errorMessage = pointerError.getString(0);
      throw new LevelDBException(errorMessage);
    }
  }
}
