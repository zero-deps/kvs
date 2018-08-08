package leveldbjnr;

import jnr.ffi.Pointer;

public class LevelDBWriteBatch implements AutoCloseable {
    protected Pointer writeBatch;

    public LevelDBWriteBatch() {
        writeBatch = LevelDB.lib.leveldb_writebatch_create();
    }

    @Override
    public void close() {
        checkWriteBatchOpen();

        LevelDB.lib.leveldb_writebatch_destroy(writeBatch);
        writeBatch = null;
    }

    public void clear() {
        checkWriteBatchOpen();

        LevelDB.lib.leveldb_writebatch_clear(writeBatch);
    }

    public void put(byte[] key, byte[] value) {
        checkWriteBatchOpen();

        if (LevelDB.is64bit()) {
            long keyLength = key != null ? key.length : 0;
            long valueLength = value != null ? value.length : 0;
            LevelDB.lib.leveldb_writebatch_put(writeBatch, key, keyLength, value, valueLength);
        } else {
            int keyLength = key != null ? key.length : 0;
            int valueLength = value != null ? value.length : 0;
            LevelDB.lib.leveldb_writebatch_put(writeBatch, key, keyLength, value, valueLength);
        }
    }

    public void delete(byte[] key) {
        checkWriteBatchOpen();

        if (LevelDB.is64bit()) {
            long keyLength = key != null ? key.length : 0;
            LevelDB.lib.leveldb_writebatch_delete(writeBatch, key, keyLength);
        } else {
            int keyLength = key != null ? key.length : 0;
            LevelDB.lib.leveldb_writebatch_delete(writeBatch, key, keyLength);
        }
    }

    protected void checkWriteBatchOpen() {
        if (writeBatch == null) {
            throw new LevelDBException("LevelDB write batch was closed.");
        }
    }
}
