package leveldbjnr;

import jnr.ffi.Pointer;
import jnr.ffi.TypeAlias;
import jnr.ffi.byref.NumberByReference;

public class LevelDBKeyIterator extends LevelDBIteratorBase<byte[]> {
    public LevelDBKeyIterator(LevelDB levelDB, LevelDBReadOptions readOptions) {
        super(levelDB, readOptions);
        LevelDB.lib.leveldb_iter_seek_to_first(iterator);
    }

    public byte[] next() {
        levelDB.checkDatabaseOpen();
        checkIteratorOpen();

        NumberByReference resultLengthPointer = new NumberByReference(TypeAlias.size_t);
        Pointer resultPointer = LevelDB.lib.leveldb_iter_key(iterator, resultLengthPointer);

        int resultLength = resultLengthPointer.intValue();

        byte[] key = new byte[resultLength];
        resultPointer.get(0, key, 0, resultLength);

        LevelDB.lib.leveldb_iter_next(iterator);

        return key;
    }
}
