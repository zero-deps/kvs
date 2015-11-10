package mws.kvs;

import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.junit.Ignore;
import org.junit.Test;
import scala.collection.immutable.$colon$colon;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import static org.junit.Assert.assertTrue;

/**
 * Test to verify that LevelDB write/read to filesystem without
 * org.iq80.leveldb.DBException: IO error: ?.sst: Could not create random access file.
 *
 * This exception occurs constantly on Windows OS.
 *
 * @see <a href="https://groups.google.com/forum/#!msg/leveldb/ue-UFo6Xt60/K1S7Ai8z_y8J">IO error when doing random reads using LevelDBJni</a>
 */
@Ignore("It's not a unit test but verifying test that LevelDB works correctly")
public class LeveldbKvsRandomReadWriteTest {

    private static final int MILLION = 1000000;

    // Random but consistent
    private static final Random random = new Random(123456L);

    //Batch size for
    private static final int BATCH_SIZE = MILLION;

    @Test
    public void testCreateAndSearch() throws IOException {
        File store = new File("leveldb-randomreadwrite");
        store.mkdirs();

        int itemIDSpace = MILLION / 3;
        createData(store, itemIDSpace);
        searchData(store, itemIDSpace);
    }

    public void createData(File store, int itemsCountToProcess)
            throws IOException {
        DB db = getDB(store);
        int ops = 0;
        int TAGS = 100;
        try {
            for (int i = 0; i < itemsCountToProcess; i++) {
                int item = random.nextInt(itemsCountToProcess) + 1;
                for (int tag = 0; tag < TAGS; tag++) {
                    byte[] data = new byte[4];
                    random.nextBytes(data);
                    long longKey = (long)item << 32 | tag & 0xFFFFFFFFL;
                    db.put(LONG_SERIALIZER.serialize(longKey), INT_SERIALIZER.serialize(random.nextInt()));
                    ops++;
                    if (ops % BATCH_SIZE == 0) {
                        System.out.println("Completed Operations=" + ops
                                + " of " + itemsCountToProcess * TAGS);
                    }
                }
            }
        } finally {
            db.close();
        }
    }

    private DB getDB(File store) throws IOException {
        Options options = new Options();
        options.createIfMissing(true);
        options.writeBufferSize(62914560);
        options.maxOpenFiles(250);
        options.cacheSize(1073741824);
        DB db = JniDBFactory.factory.open(store, options);
        return db;
    }

    public void searchData(File store, int itemIDSpace) throws IOException {
        DB db = getDB(store);
        int hits = 0;
        for (int i = 0; i < itemIDSpace; i++) {
            long id = random.nextInt(itemIDSpace) + 1;
            id = id << 32; // Uses tagid as zero
            try {
                if (db.get(LONG_SERIALIZER.serialize(id)) != null)
                    hits++;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        // Just to make sure some hits
        assertTrue(hits > itemIDSpace / 2);

    }


    public interface Serializer<T> {

        public byte[] serialize(T value);

        public T deserialize(byte[] bytes);

    }


    /**
     * Serializes Integers
     */
    public static final Serializer<Integer> INT_SERIALIZER = new Serializer<Integer>() {

        @Override
        public byte[] serialize(Integer value) {
            if (value == null) {
                return null;
            }

            return ByteBuffer.allocate(4).putInt(value).array();
        }

        @Override
        public Integer deserialize(byte[] bytes) {
            if (bytes == null) {
                return null;
            }

            return ByteBuffer.wrap(bytes).getInt();
        }
    };

    /**
     * Serialize long values
     */
    public static final Serializer<Long> LONG_SERIALIZER = new Serializer<Long>() {

        @Override
        public byte[] serialize(Long value) {
            if (value == null) {
                return null;
            }

            return ByteBuffer.allocate(8).putLong(value).array();
        }

        @Override
        public Long deserialize(byte[] bytes) {
            if (bytes == null) {
                return null;
            }

            return ByteBuffer.wrap(bytes).getLong();
        }
    };

}