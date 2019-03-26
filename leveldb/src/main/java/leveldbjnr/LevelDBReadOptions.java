package leveldbjnr;

import jnr.ffi.Pointer;

public class LevelDBReadOptions implements AutoCloseable {
    protected Pointer readOptions;

    private boolean verifyChecksum = false;
    private boolean fillCache = true;

    public LevelDBReadOptions() {
        readOptions = LevelDB.lib.leveldb_readoptions_create();
        setVerifyChecksum(verifyChecksum);
        setFillCache(fillCache);
    }

    public void close() {
        checkReadOptionsOpen();

        LevelDB.lib.leveldb_readoptions_destroy(readOptions);
        readOptions = null;
    }

    public boolean isVerifyChecksum() {
        return verifyChecksum;
    }

    public void setVerifyChecksum(boolean verifyChecksum) {
        checkReadOptionsOpen();

        this.verifyChecksum = verifyChecksum;
        LevelDB.lib.leveldb_readoptions_set_verify_checksums(readOptions, (byte) (verifyChecksum ? 1 : 0));
    }

    public boolean isFillCache() {
        return fillCache;
    }

    public void setFillCache(boolean fillCache) {
        checkReadOptionsOpen();

        this.fillCache = fillCache;
        LevelDB.lib.leveldb_readoptions_set_fill_cache(readOptions, (byte) (fillCache ? 1 : 0));
    }

    protected void checkReadOptionsOpen() {
        if (readOptions == null) {
            throw new LevelDBException("LevelDB read options was closed.");
        }
    }
}
