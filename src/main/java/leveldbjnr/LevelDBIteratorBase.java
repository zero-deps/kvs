package leveldbjnr;

import jnr.ffi.Pointer;
import jnr.ffi.byref.PointerByReference;

import java.util.Iterator;

public abstract class LevelDBIteratorBase<TElement> implements AutoCloseable, Iterator<TElement> {
    protected LevelDB levelDB;
    protected Pointer iterator;

    public LevelDBIteratorBase(LevelDB levelDB, LevelDBReadOptions readOptions) {
        this.levelDB = levelDB;

        levelDB.checkDatabaseOpen();
        readOptions.checkReadOptionsOpen();
        iterator = LevelDB.lib.leveldb_create_iterator(levelDB.levelDB, readOptions.readOptions);
    }

    public void close() {
        levelDB.checkDatabaseOpen();
        checkIteratorOpen();

        LevelDB.lib.leveldb_iter_destroy(iterator);
        iterator = null;
    }

    public boolean hasNext() {
        levelDB.checkDatabaseOpen();
        checkIteratorOpen();

        boolean hasNext = LevelDB.lib.leveldb_iter_valid(iterator) != 0;
        checkError();
        return hasNext;
    }

    public void seekToFirst() {
        levelDB.checkDatabaseOpen();
        checkIteratorOpen();

        LevelDB.lib.leveldb_iter_seek_to_first(iterator);
        checkError();
    }

    public void seekToLast() {
        levelDB.checkDatabaseOpen();
        checkIteratorOpen();

        LevelDB.lib.leveldb_iter_seek_to_last(iterator);
        checkError();
    }

    public void seekToKey(byte[] key) {
        levelDB.checkDatabaseOpen();
        checkIteratorOpen();

        if (LevelDB.is64bit()) {
            long keyLength = key != null ? key.length : 0;
            LevelDB.lib.leveldb_iter_seek(iterator, key, keyLength);
        } else {
            int keyLength = key != null ? key.length : 0;
            LevelDB.lib.leveldb_iter_seek(iterator, key, keyLength);
        }
        checkError();
    }

    private void checkError() {
        PointerByReference error = new PointerByReference();
        LevelDB.lib.leveldb_iter_get_error(iterator, error);
        LevelDB.checkError(error);
    }

    protected void checkIteratorOpen() {
        if (iterator == null) {
            throw new LevelDBException("LevelDB iterator was closed.");
        }
    }
}
