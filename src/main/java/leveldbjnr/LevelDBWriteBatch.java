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

        LevelDB.lib.leveldb_writebatch_put(writeBatch, key, key.length, value, value.length);
    }

    public void delete(byte[] key) {
        checkWriteBatchOpen();

        LevelDB.lib.leveldb_writebatch_delete(writeBatch, key, key.length);
    }

    protected void checkWriteBatchOpen() {
        if (writeBatch == null) throw new LevelDBException("LevelDB write batch was closed.");
    }
}
