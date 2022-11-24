package org.hudi;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.io.hfile.*;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.hudi.bloom.BloomFilter;
import org.hudi.mine.StoreConfig;
import org.hudi.storage.HoodieFileReader;
import org.hudi.storage.HoodieHFileReader;
import org.junit.Assert;
import org.junit.Test;
import org.test.DateTimeUtils;
import org.test.User;
import org.util.Option;
import org.util.RandomKeyValueUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.hadoop.hbase.HConstants.BUCKET_CACHE_IOENGINE_KEY;
import static org.apache.hadoop.hbase.HConstants.BUCKET_CACHE_SIZE_KEY;
import static org.apache.hadoop.hbase.io.ByteBuffAllocator.*;
import static org.apache.hadoop.hbase.io.hfile.BlockCacheFactory.BLOCKCACHE_POLICY_KEY;
import static org.apache.hadoop.hbase.io.hfile.CacheConfig.EVICT_BLOCKS_ON_CLOSE_KEY;

public class TestHfileRead {
    private static Configuration conf = StoreConfig.getConf();

    public static void main(String[] args) throws Exception {
        int total = 1000;
        String path = "/user/data/files/";
        String dataPath = path + "bloom.hfile";
        Random random = new Random();
        Path indexPath = new Path(dataPath);

        HoodieHFileReader hoodieHFileReader = (HoodieHFileReader) createReaderWithComb(indexPath);
//        HoodieHFileReader hoodieHFileReader = (HoodieHFileReader) TestBloom.createReader(indexPath);
        long times = 0, exists = 0, might = 0;
        for (int i = 0; i < total; i++) {
            String rowkey = User.getKey(random.nextInt(5 * 10000));
            long start = System.nanoTime();
            BloomFilter filter = hoodieHFileReader.readBloomFilter();
            if (filter.mightContain(rowkey)) {
                might++;
                Option recordByKey = hoodieHFileReader.getRecordByKey(rowkey);
                if (recordByKey.isPresent()) {
                    exists++;
                }
                long time = DateTimeUtils.getCostMs(start);
                times += time;
                System.out.println("---read cost:" + time + " ms");
            }

        }
        System.out.println("---read avg cost:" + times / total + " ms,exists:" + exists + " ,might:" + might);
    }

    @Test
    public void readHfile() throws Exception {
        String path = "/user/data/files/";
        String dataPath = path + "bloom.hfile";
        Path indexPath = new Path(dataPath);
        HoodieHFileReader hoodieHFileReader = (HoodieHFileReader) createReaderWithComb(indexPath);
        List<String> rowsList = new ArrayList<>();
        Collections.sort(rowsList);
        List<GenericRecord> result = HoodieHFileReader.readRecords(hoodieHFileReader, rowsList);
//        List<GenericRecord> result = HoodieHFileReader.readAllRecords(hoodieHFileReader);
        //bloom filter
        BloomFilter filter = hoodieHFileReader.readBloomFilter();
        int mightContain = 0, actualContain = 0;
        for (String rowkey : rowsList) {
            if (filter.mightContain(rowkey)) {
                mightContain++;
            }
        }

//        for(GenericRecord record : result){
//            System.out.println(record.toString());
//        }
        System.out.println("mightContain size:" + mightContain + ",actualContain size:" + result.size() + ",rowKeys:" + rowsList.size());
    }

    @Test
    public void testReaderWithLRUBlockCache() throws Exception {
        conf.set(HConstants.HBASE_DIR, "/user/data/files/storefile/");
        FileSystem fs = HFileSystem.get(conf);
        Path storeFilePath = writeStoreFile(fs);
//        String path = "/user/data/files/storefile/e85453fbf999449580cfacb1f107d160";
//        Path storeFilePath = new Path(path);
        int bufCount = 1024, blockSize = 64 * 1024;
        ByteBuffAllocator alloc = initAllocator(true, bufCount, blockSize, 0);
        fillByteBuffAllocator(alloc, bufCount);
        // Open the file reader with LRUBlockCache
        BlockCache lru = new LruBlockCache(1024 * 1024 * 32, blockSize, true, conf);
        CacheConfig cacheConfig = new CacheConfig(conf, null, lru, alloc);
        HFile.Reader reader = HFile.createReader(fs, storeFilePath, cacheConfig, true, conf);
        long offset = 0;
        while (offset < reader.getTrailer().getLoadOnOpenDataOffset()) {
            BlockCacheKey key = new BlockCacheKey(storeFilePath.getName(), offset);
            HFileBlock block = reader.readBlock(offset, -1, true, true, false, true, null, null);
            offset += block.getOnDiskSizeWithHeader();
            // Ensure the block is an heap one.
            Cacheable cachedBlock = lru.getBlock(key, false, false, true);
            Assert.assertNotNull(cachedBlock);
            Assert.assertTrue(cachedBlock instanceof HFileBlock);
            Assert.assertFalse(((HFileBlock) cachedBlock).isSharedMem());
            // Should never allocate off-heap block from allocator because ensure that it's LRU.
            Assert.assertEquals(bufCount, alloc.getFreeBufferCount());
            block.release(); // return back the ByteBuffer back to allocator.
        }
        reader.close();
        Assert.assertEquals(bufCount, alloc.getFreeBufferCount());
        alloc.clean();
        lru.shutdown();
    }

    private Path writeStoreFile(FileSystem fs) throws IOException {
        String path = "/user/data/files/storefile";
        Path storeFileParentDir = new Path(path);
        HFileContext meta = new HFileContextBuilder().withBlockSize(64 * 1024).build();
        StoreFileWriter sfw = new StoreFileWriter.Builder(conf, fs).withOutputDir(storeFileParentDir)
                .withFileContext(meta).withBloomType(BloomType.ROW).build();
        final int rowLen = 32;
        Random rand = ThreadLocalRandom.current();
        for (int i = 0; i < 5; ++i) {
            byte[] k = RandomKeyValueUtil.randomOrderedKey(rand, i);
            byte[] v = RandomKeyValueUtil.randomValue(rand);
            int cfLen = rand.nextInt(k.length - rowLen + 1);
            KeyValue kv = new KeyValue(k, 0, rowLen, k, rowLen, cfLen, k, rowLen + cfLen,
                    k.length - rowLen - cfLen, rand.nextLong(), generateKeyType(rand), v, 0, v.length);
            sfw.append(kv);
        }

        sfw.close();
        return sfw.getPath();
    }

    @Test
    public void testReaderWithLRUBlockCacheMy() throws Exception {
        String path = "/user/data/files/storefile/e85453fbf999449580cfacb1f107d160";
        Path storeFilePath = new Path(path);

        int bufCount = 1024, blockSize = 64 * 1024;
        ByteBuffAllocator alloc = initAllocator(true, bufCount, blockSize, 0);
        fillByteBuffAllocator(alloc, bufCount);
        // Open the file reader with LRUBlockCache
        BlockCache lru = new LruBlockCache(1024 * 1024 * 32, blockSize, true, conf);
        CacheConfig cacheConfig = new CacheConfig(conf, null, lru, alloc);
        HoodieHFileReader<IndexedRecord> hoodieHFileReader = new HoodieHFileReader<>(conf, storeFilePath, cacheConfig, storeFilePath.getFileSystem(conf));
        long offset = 0;
        while (offset < hoodieHFileReader.readTrailer()) {
            BlockCacheKey key = new BlockCacheKey(storeFilePath.getName(), offset);
            HFileBlock block = hoodieHFileReader.readBlock(offset);
            offset += block.getOnDiskSizeWithHeader();
            // Ensure the block is an heap one.
            Cacheable cachedBlock = lru.getBlock(key, false, false, true);
            Assert.assertNotNull(cachedBlock);
            Assert.assertTrue(cachedBlock instanceof HFileBlock);
            Assert.assertFalse(((HFileBlock) cachedBlock).isSharedMem());
            // Should never allocate off-heap block from allocator because ensure that it's LRU.
            Assert.assertEquals(bufCount, alloc.getFreeBufferCount());
            block.release(); // return back the ByteBuffer back to allocator.
        }
        hoodieHFileReader.close();
        Assert.assertEquals(bufCount, alloc.getFreeBufferCount());
        alloc.clean();
        lru.shutdown();
    }

    /**
     * Test case for HBASE-22127 in CombinedBlockCache
     */
    @Test
    public void testReaderWithCombinedBlockCache() throws Exception {
        String path = "/home/bigdata/data/storefile/96db21b0c64d4d5db113489fc202b29e";
        Path indexPath = new Path(path);

        int bufCount = 1024 * 10, blockSize = 1024 * 1024;
        ByteBuffAllocator alloc = initAllocator(true, bufCount, blockSize, 0);
        fillByteBuffAllocator(alloc, bufCount);
        // Open the file reader with LRUBlockCache
//        BlockCache lru = new LruBlockCache(1024 * 1024 * 32, blockSize, true, conf);
//        CacheConfig cacheConfig = new CacheConfig(conf, null, lru, alloc);

        // Open the file reader with CombinedBlockCache
        BlockCache combined = initCombinedBlockCache("LRU");
        conf.setBoolean(EVICT_BLOCKS_ON_CLOSE_KEY, true);
        CacheConfig cacheConfig = new CacheConfig(conf, null, combined, alloc);

        HoodieHFileReader<IndexedRecord> hoodieHFileReader = new HoodieHFileReader<>(conf, indexPath, cacheConfig, indexPath.getFileSystem(conf));

        long offset = 0;
        while (offset < hoodieHFileReader.readTrailer()) {
            BlockCacheKey key = new BlockCacheKey(indexPath.getName(), offset);
            HFileBlock block = hoodieHFileReader.readBlock(offset);
            offset += block.getOnDiskSizeWithHeader();
            // Read the cached block.
            Cacheable cachedBlock = combined.getBlock(key, false, false, true);
            try {
                Assert.assertNotNull(cachedBlock);
                Assert.assertTrue(cachedBlock instanceof HFileBlock);
                HFileBlock hfb = (HFileBlock) cachedBlock;
                // Data block will be cached in BucketCache, so it should be an off-heap block.
                if (hfb.getBlockType().isData()) {
                    Assert.assertTrue(hfb.isSharedMem());
                } else {
                    // Non-data block will be cached in LRUBlockCache, so it must be an on-heap block.
                    Assert.assertFalse(hfb.isSharedMem());
                }
            } finally {
                cachedBlock.release();
            }
            block.release(); // return back the ByteBuffer back to allocator.
        }
        hoodieHFileReader.close();
        combined.shutdown();
        Assert.assertEquals(bufCount, alloc.getFreeBufferCount());
        alloc.clean();
    }

    protected static HoodieFileReader<GenericRecord> createReaderWithLRU(Path path) throws Exception {
        int bufCount = 1024*10, blockSize = 1024 * 1024;
        ByteBuffAllocator alloc = initAllocator(true, bufCount, blockSize, 0);
        fillByteBuffAllocator(alloc, bufCount);
        // Open the file reader with LRUBlockCache
        BlockCache lru = new LruBlockCache(1024 * 1024 * 32, blockSize, true, conf);
        CacheConfig cacheConfig = new CacheConfig(conf, null, lru, alloc);

        return new HoodieHFileReader<>(conf, path, cacheConfig, path.getFileSystem(conf));
    }

    public static ByteBuffAllocator initAllocator(boolean reservoirEnabled, int bufSize, int bufCount,
                                            int minAllocSize) {
        Configuration that = HBaseConfiguration.create(conf);
        that.setInt(BUFFER_SIZE_KEY, bufSize);
        that.setInt(MAX_BUFFER_COUNT_KEY, bufCount);
        // All ByteBuffers will be allocated from the buffers.
        that.setInt(MIN_ALLOCATE_SIZE_KEY, minAllocSize);
        return ByteBuffAllocator.create(that, reservoirEnabled);
    }

    public static void fillByteBuffAllocator(ByteBuffAllocator alloc, int bufCount) {
        // Fill the allocator with bufCount ByteBuffer
        List<ByteBuff> buffs = new ArrayList<>();
        for (int i = 0; i < bufCount; i++) {
            buffs.add(alloc.allocateOneBuffer());
            Assert.assertEquals(alloc.getFreeBufferCount(), 0);
        }
        buffs.forEach(ByteBuff::release);
        Assert.assertEquals(alloc.getFreeBufferCount(), bufCount);
    }

    private static BlockCache initCombinedBlockCache(final String l1CachePolicy) {
        Configuration that = HBaseConfiguration.create(conf);
        that.setFloat(BUCKET_CACHE_SIZE_KEY, 32); // 32MB for bucket cache.
        that.set(BUCKET_CACHE_IOENGINE_KEY, "offheap");
        that.set(BLOCKCACHE_POLICY_KEY, l1CachePolicy);
        BlockCache bc = BlockCacheFactory.createBlockCache(that);
        Assert.assertNotNull(bc);
        Assert.assertTrue(bc instanceof CombinedBlockCache);
        return bc;
    }

    protected static HoodieFileReader<GenericRecord> createReaderWithComb(Path path) throws Exception {
        int bufCount = 1024, blockSize = 64 * 1024;
        ByteBuffAllocator alloc = initAllocator(true, bufCount, blockSize, 0);
        fillByteBuffAllocator(alloc, bufCount);

        // Open the file reader with CombinedBlockCache
        BlockCache combined = initCombinedBlockCache("LRU");
        conf.setBoolean(EVICT_BLOCKS_ON_CLOSE_KEY, true);
        CacheConfig cacheConfig = new CacheConfig(conf, null, combined, alloc);

        return new HoodieHFileReader<>(conf, path, cacheConfig, path.getFileSystem(conf));
    }


    private static final int NUM_VALID_KEY_TYPES = KeyValue.Type.values().length - 2;
    public static KeyValue.Type generateKeyType(Random rand) {
        if (rand.nextBoolean()) {
            // Let's make half of KVs puts.
            return KeyValue.Type.Put;
        } else {
            KeyValue.Type keyType = KeyValue.Type.values()[1 + rand.nextInt(NUM_VALID_KEY_TYPES)];
            if (keyType == KeyValue.Type.Minimum || keyType == KeyValue.Type.Maximum) {
                throw new RuntimeException("Generated an invalid key type: " + keyType + ". "
                        + "Probably the layout of KeyValue.Type has changed.");
            }
            return keyType;
        }
    }

}
