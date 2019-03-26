package leveldbjnr;

import jnr.ffi.Pointer;

public class LevelDBWriteOptions implements AutoCloseable {
    protected Pointer writeOptions;

    private boolean sync = false;

    public LevelDBWriteOptions() {
        writeOptions = LevelDB.lib.leveldb_writeoptions_create();
        setSync(sync);
    }

    public void close() {
        checkWriteOptionsOpen();

        LevelDB.lib.leveldb_writeoptions_destroy(writeOptions);
        writeOptions = null;
    }

    public boolean isSync() {
        return sync;
    }

    public void setSync(boolean sync) {
        checkWriteOptionsOpen();

        this.sync = sync;
        LevelDB.lib.leveldb_writeoptions_set_sync(writeOptions, (byte) (sync ? 1 : 0));
    }

    protected void checkWriteOptionsOpen() {
        if (writeOptions == null) {
            throw new LevelDBException("LevelDB write options was closed.");
        }
    }
}
