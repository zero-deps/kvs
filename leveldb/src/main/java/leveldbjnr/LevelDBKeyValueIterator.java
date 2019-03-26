package leveldbjnr;

import jnr.ffi.Pointer;
import jnr.ffi.TypeAlias;
import jnr.ffi.byref.NumberByReference;

public class LevelDBKeyValueIterator extends LevelDBIteratorBase<KeyValuePair> {
    public LevelDBKeyValueIterator(LevelDB levelDB, LevelDBReadOptions readOptions) {
        super(levelDB, readOptions);
        LevelDB.lib.leveldb_iter_seek_to_first(iterator);
    }

    public KeyValuePair next() {
        levelDB.checkDatabaseOpen();
        checkIteratorOpen();

        Pointer resultPointer;
        NumberByReference resultLengthPointer = new NumberByReference(TypeAlias.size_t);
        int resultLength;

        resultPointer = LevelDB.lib.leveldb_iter_key(iterator, resultLengthPointer);
        resultLength = resultLengthPointer.intValue();
        byte[] key = new byte[resultLength];
        resultPointer.get(0, key, 0, resultLength);

        resultPointer = LevelDB.lib.leveldb_iter_value(iterator, resultLengthPointer);
        resultLength = resultLengthPointer.intValue();
        byte[] value = new byte[resultLength];
        resultPointer.get(0, value, 0, resultLength);

        LevelDB.lib.leveldb_iter_next(iterator);

        return new KeyValuePair(key, value);
    }
}
