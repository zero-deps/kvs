package leveldbjnr;

import jnr.ffi.Pointer;

public class LevelDBOptions implements AutoCloseable {
    protected Pointer options;

    private boolean createIfMissing = false;
    private boolean errorIfExists = false;
    private boolean paranoidChecks = false;
    private LevelDBCompressionType compressionType = LevelDBCompressionType.SnappyCompression;
    private long writeBufferSize = 4 * 1024 * 1204;
    private int maxOpenFiles = 1000;
    private long blockSize = 4096;
    private int blockRestartInterval = 16;

    public LevelDBOptions() {
        options = LevelDB.lib.leveldb_options_create();
        setCreateIfMissing(createIfMissing);
        setErrorIfExists(errorIfExists);
        setParanoidChecks(paranoidChecks);
        setCompressionType(compressionType);
        setWriteBufferSize(writeBufferSize);
        setMaxOpenFiles(maxOpenFiles);
        setBlockSize(blockSize);
        setBlockRestartInterval(blockRestartInterval);
    }

    public void close() {
        checkOptionsOpen();

        LevelDB.lib.leveldb_options_destroy(options);
        options = null;
    }

    public boolean isCreateIfMissing() {
        return createIfMissing;
    }

    public void setCreateIfMissing(boolean createIfMissing) {
        checkOptionsOpen();

        this.createIfMissing = createIfMissing;
        LevelDB.lib.leveldb_options_set_create_if_missing(options, (byte) (createIfMissing ? 1 : 0));
    }

    public boolean isErrorIfExists() {
        return errorIfExists;
    }

    public void setErrorIfExists(boolean errorIfExists) {
        checkOptionsOpen();

        this.errorIfExists = errorIfExists;
        LevelDB.lib.leveldb_options_set_error_if_exists(options, (byte) (errorIfExists ? 1 : 0));
    }

    public boolean isParanoidChecks() {
        return paranoidChecks;
    }

    public void setParanoidChecks(boolean paranoidChecks) {
        checkOptionsOpen();

        this.paranoidChecks = paranoidChecks;
        LevelDB.lib.leveldb_options_set_paranoid_checks(options, (byte) (paranoidChecks ? 1 : 0));
    }

    public LevelDBCompressionType getCompressionType() {
        return compressionType;
    }

    public void setCompressionType(LevelDBCompressionType compressionType) {
        checkOptionsOpen();

        this.compressionType = compressionType;
        LevelDB.lib.leveldb_options_set_compression(options, compressionType.getCompressionType());
    }

    public long getWriteBufferSize() {
        return writeBufferSize;
    }

    public void setWriteBufferSize(long writeBufferSize) {
        checkOptionsOpen();

        this.writeBufferSize = writeBufferSize;
        LevelDB.lib.leveldb_options_set_write_buffer_size(options, writeBufferSize);
    }

    public int getMaxOpenFiles() {
        return maxOpenFiles;
    }

    public void setMaxOpenFiles(int maxOpenFiles) {
        checkOptionsOpen();

        this.maxOpenFiles = maxOpenFiles;
        LevelDB.lib.leveldb_options_set_max_open_files(options, maxOpenFiles);
    }

    public long getBlockSize() {
        return blockSize;
    }

    public void setBlockSize(long blockSize) {
        checkOptionsOpen();

        this.blockSize = blockSize;
        LevelDB.lib.leveldb_options_set_block_size(options, blockSize);
    }

    public int getBlockRestartInterval() {
        return blockRestartInterval;
    }

    public void setBlockRestartInterval(int blockRestartInterval) {
        checkOptionsOpen();

        this.blockRestartInterval = blockRestartInterval;
        LevelDB.lib.leveldb_options_set_block_restart_interval(options, blockRestartInterval);
    }

    protected void checkOptionsOpen() {
        if (options == null) {
            throw new LevelDBException("LevelDB options was closed.");
        }
    }
}
