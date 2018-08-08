package leveldbjnr;

import jnr.ffi.Pointer;
import jnr.ffi.LibraryLoader;
import jnr.ffi.Runtime;
import jnr.ffi.TypeAlias;
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
            File dest = new File("./lib/" + name);
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
        System.setProperty("java.library.path", "./lib/");
    }
    public static final LevelDBNative lib = LibraryLoader.create(LevelDBNative.class).failImmediately().load("leveldb");
    public static final Runtime runtime = Runtime.getRuntime(lib);
    public static boolean is64bit() {
        return runtime.addressSize() == 8;
    } 

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

        Pointer result;
        int resultLength;
        PointerByReference error = new PointerByReference();
        if (is64bit()) {
            long keyLength = key != null ? key.length : 0;
            result = LevelDB.lib.leveldb_get(levelDB, readOptions.readOptions, key, keyLength, resultLengthPointer, error);
            checkError(error);
            resultLength = resultLengthPointer.intValue();
        } else {
            int keyLength = key != null ? key.length : 0;
            result = LevelDB.lib.leveldb_get(levelDB, readOptions.readOptions, key, keyLength, resultLengthPointer, error);
            checkError(error);
            resultLength = resultLengthPointer.intValue();
        }

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
        if (is64bit()) {
            long keyLength = key != null ? key.length : 0;
            long valueLength = value != null ? value.length : 0;
            LevelDB.lib.leveldb_put(levelDB, writeOptions.writeOptions, key, keyLength, value, valueLength, error);
        } else {
            int keyLength = key != null ? key.length : 0;
            int valueLength = value != null ? value.length : 0;
            LevelDB.lib.leveldb_put(levelDB, writeOptions.writeOptions, key, keyLength, value, valueLength, error);
        }
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
        if (is64bit()) {
            long keyLength = key != null ? key.length : 0;
            LevelDB.lib.leveldb_delete(levelDB, writeOptions.writeOptions, key, keyLength, error);
        } else {
            int keyLength = key != null ? key.length : 0;
            LevelDB.lib.leveldb_delete(levelDB, writeOptions.writeOptions, key, keyLength, error);
        }
        LevelDB.checkError(error);
    }

    public String property(String property) {
        checkDatabaseOpen();
        return LevelDB.lib.leveldb_property_value(levelDB, property);
    }

    // public long[] approximateSizes(Range ...ranges) {
    //     checkDatabaseOpen();

    //     if (ranges.length == 0)
    //         return new long[0];

    //     Memory startKeys = new Memory(ranges.length * Pointer.SIZE);
    //     Memory limitKeys = new Memory(ranges.length * Pointer.SIZE);

    //     for (int i = 0; i < ranges.length; i++) {
    //         int startKeyLength = ranges[i].getStartKey().length;
    //         Memory startKeyMemory = new Memory(startKeyLength);
    //         startKeyMemory.write(0, ranges[i].getStartKey(), 0, startKeyLength);

    //         startKeys.setPointer(i * Pointer.SIZE, startKeyMemory);

    //         int limitKeyLength = ranges[i].getLimitKey().length;
    //         Memory limitKeyMemory = new Memory(limitKeyLength);
    //         limitKeyMemory.write(0, ranges[i].getLimitKey(), 0, limitKeyLength);

    //         limitKeys.setPointer(i * Pointer.SIZE, limitKeyMemory);
    //     }

    //     Pointer sizes = new Memory(ranges.length * Native.getNativeSize(Long.TYPE));
    //     if (is64bit()) {
    //         long[] startLengths = new long[ranges.length];
    //         long[] limitLengths = new long[ranges.length];

    //         for (int i = 0; i < ranges.length; i++) {
    //             startLengths[i] = ranges[i].getStartKey().length;
    //             limitLengths[i] = ranges[i].getLimitKey().length;
    //         }

    //         LevelDB.lib.leveldb_approximate_sizes(levelDB, ranges.length, startKeys, startLengths, limitKeys, limitLengths, sizes);
    //     } else {
    //         int[] startLengths = new int[ranges.length];
    //         int[] limitLengths = new int[ranges.length];

    //         for (int i = 0; i < ranges.length; i++) {
    //             startLengths[i] = ranges[i].getStartKey().length;
    //             limitLengths[i] = ranges[i].getLimitKey().length;
    //         }

    //         LevelDB.lib.leveldb_approximate_sizes(levelDB, ranges.length, startKeys, startLengths, limitKeys, limitLengths, sizes);
    //     }

    //     return sizes.getLongArray(0, ranges.length);
    // }

    public void compactRange(byte[] startKey, byte[] limitKey) {
        checkDatabaseOpen();

        if (is64bit()) {
            long startKeyLength = startKey != null ? startKey.length : 0;
            long limitKeyLength = limitKey != null ? limitKey.length : 0;
            LevelDB.lib.leveldb_compact_range(levelDB, startKey, startKeyLength, limitKey, limitKeyLength);
        } else {
            int startKeyLength = startKey != null ? startKey.length : 0;
            int limitKeyLength = limitKey != null ? limitKey.length : 0;
            LevelDB.lib.leveldb_compact_range(levelDB, startKey, startKeyLength, limitKey, limitKeyLength);
        }
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
        if (levelDB == null) {
            throw new LevelDBException("LevelDB database was closed.");
        }
    }

    public static void checkError(PointerByReference error) {
        Pointer pointerError = error.getValue();
        if (pointerError != null) {
            String errorMessage = pointerError.getString(0);
            throw new LevelDBException(errorMessage);
        }
    }
}
