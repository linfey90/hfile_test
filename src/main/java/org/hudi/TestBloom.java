package org.hudi;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.LruBlockCache;
import org.hudi.bloom.BloomFilter;
import org.hudi.mine.StoreConfig;
import org.hudi.storage.HoodieFileReader;
import org.hudi.storage.HoodieFileWriter;
import org.hudi.storage.HoodieFileWriterFactory;
import org.hudi.storage.HoodieHFileReader;
import org.junit.Test;
import org.mockito.Mockito;
import org.test.User;
import org.util.SchemaTestUtil;
import org.util.TaskContextSupplier;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;
public class TestBloom {
    protected static final int NUM_RECORDS = 50;
    public static final Random RANDOM = new Random();

    protected static Path getFilePath() {
//        return new Path("/home/bigdata/data/my.hfile");
        return new Path("/user/data/files/bloom2.hfile");
    }


    @Test
    public void readHfile() throws Exception {
        HoodieHFileReader hoodieHFileReader = (HoodieHFileReader) createReader(getFilePath());
        List<String> rowsList = new ArrayList<>(getTestKeys(500000000));
        Collections.sort(rowsList);
        List<GenericRecord> result = HoodieHFileReader.readRecords(hoodieHFileReader, rowsList);
//        List<GenericRecord> result = HoodieHFileReader.readAllRecords(hoodieHFileReader);
        //bloom filter
        BloomFilter filter = hoodieHFileReader.readBloomFilter();
        int mightContain = 0, actualContain = 0;
        for(String rowkey : rowsList){
            if (filter.mightContain(rowkey)) {
                mightContain++;
            }
        }

//        for(GenericRecord record : result){
//            System.out.println(record.toString());
//        }
        System.out.println("mightContain size:"+mightContain+",actualContain size:"+result.size()+",rowKeys:"+rowsList.size());
    }

    private static Set<String> getTestKeys(int start) {
        Set<String> rowKeys = new HashSet<>();
        for(int i=start;i< start+15000;i++){
            User user = new User(i);
            rowKeys.add(user.getKey());
        }
        return rowKeys;
    }

    @Test
    public void writeEmpty() throws Exception {
        boolean populateMetaFields= false;
        int total = 500*10000;
        Schema avroSchema = SchemaTestUtil.getSchemaFromResource(TestBloom.class, "/exampleSchemaWithMetaFields.avsc");
        HoodieFileWriter<GenericRecord> writer = createWriter(avroSchema, getFilePath(), populateMetaFields);
        for (int i = 0; i < total; i++) {
            User user = new User(i);
            GenericRecord record = new GenericData.Record(avroSchema);
            record.put("id", user.getId()+"");
            record.put("name", user.getName());
            record.put("path", user.getAddress());
            writer.writeAvro(user.getKey(i), null);
        }
        writer.close();
    }

    @Test
    public void testWriteReadHFileWithMetaFields() throws Exception {
        boolean populateMetaFields= false;
        Schema avroSchema = SchemaTestUtil.getSchemaFromResource(TestBloom.class, "/exampleSchemaWithMetaFields.avsc");
        HoodieFileWriter<GenericRecord> writer = createWriter(avroSchema, getFilePath(), populateMetaFields);
        List<String> keys = new ArrayList<>();
        Map<String, GenericRecord> recordMap = new TreeMap<>();
        for (int i = 0; i < 100; i++) {
            GenericRecord record = new GenericData.Record(avroSchema);
            String key = String.format("%s%04d", "key", i);
            record.put("_row_key", key);
            record.put("time", Integer.toString(RANDOM.nextInt()));
            record.put("number", i);
            record.put("_hoodie_operation", "ttttttttttttttest");
            writer.writeAvro(key, record);
            recordMap.put(key, record);
            keys.add(key);
        }
        writer.close();

        HoodieHFileReader hoodieHFileReader = (HoodieHFileReader) createReader(getFilePath());
        List<IndexedRecord> records = HoodieHFileReader.readAllRecords(hoodieHFileReader);
        assertEquals(new ArrayList<>(recordMap.values()), records);

        //bloom filter
        BloomFilter filter = hoodieHFileReader.readBloomFilter();
        for (int i = 0; i < 100; i++) {
            String key = String.format("%s%04d", "key", i);
            assertTrue(filter.mightContain(key));
        }

        hoodieHFileReader.close();

        for (int i = 0; i < 2; i++) {
            int randomRowstoFetch = 5 + RANDOM.nextInt(10);
            Set<String> rowsToFetch = getRandomKeys(randomRowstoFetch, keys);

            List<String> rowsList = new ArrayList<>(rowsToFetch);
            Collections.sort(rowsList);

            List<GenericRecord> expectedRecords = rowsList.stream().map(recordMap::get).collect(Collectors.toList());

            hoodieHFileReader = (HoodieHFileReader<GenericRecord>) createReader(getFilePath());
            List<GenericRecord> result = HoodieHFileReader.readRecords(hoodieHFileReader, rowsList);

            assertEquals(expectedRecords, result);
            String RECORD_KEY_METADATA_FIELD = "_hoodie_record_key";
            result.forEach(entry -> {
                if (populateMetaFields) {
                    assertNotNull(entry.get(RECORD_KEY_METADATA_FIELD));
                } else {
                    assertNull(entry.get(RECORD_KEY_METADATA_FIELD));
                }
            });
            hoodieHFileReader.close();
        }
    }

    protected static HoodieFileReader<GenericRecord> createReader(Path path) throws Exception {
        Configuration conf = StoreConfig.getConf();
        CacheConfig cacheConfig = new CacheConfig(conf);

        return new HoodieHFileReader<>(conf, path, cacheConfig, path.getFileSystem(conf));
    }
    // block cache
    protected static HoodieFileReader<GenericRecord> createReaderWithCache(Path path) throws Exception {
        Configuration conf = StoreConfig.getConf();
        int blockSize = 64 * 1024;
        // Open the file reader with LRUBlockCache
        BlockCache lru = new LruBlockCache(1024 * 1024 * 32, blockSize, true, conf);
        CacheConfig cacheConfig = new CacheConfig(conf, null, lru, null);

        return new HoodieHFileReader<>(conf, path, cacheConfig, path.getFileSystem(conf));
    }

    protected static HoodieFileWriter<GenericRecord> createWriter(
            Schema avroSchema, Path path, boolean populateMetaFields) throws Exception {
        String instantTime = "000";
        Configuration conf = StoreConfig.getConf();

        TaskContextSupplier mockTaskContextSupplier = Mockito.mock(TaskContextSupplier.class);
        Supplier<Integer> partitionSupplier = Mockito.mock(Supplier.class);
        when(mockTaskContextSupplier.getPartitionIdSupplier()).thenReturn(partitionSupplier);
        when(partitionSupplier.get()).thenReturn(10);

        return HoodieFileWriterFactory.newHFileFileWriter(
                instantTime, path, avroSchema, conf, mockTaskContextSupplier, populateMetaFields);
    }

    private static Set<String> getRandomKeys(int count, List<String> keys) {
        Set<String> rowKeys = new HashSet<>();
        int totalKeys = keys.size();
        while (rowKeys.size() < count) {
            int index = RANDOM.nextInt(totalKeys);
            if (!rowKeys.contains(index)) {
                rowKeys.add(keys.get(index));
            }
        }
        return rowKeys;
    }

    @Test
    public void testWriteReadMetadata() throws Exception {
        Schema avroSchema = SchemaTestUtil.getSchemaFromResource(TestBloom.class, "/exampleSchema.avsc");
//        writeFileWithSimpleSchema(avroSchema);

        HoodieFileReader<GenericRecord> hoodieReader = createReader(getFilePath());
        BloomFilter filter = hoodieReader.readBloomFilter();
        for (int i = 0; i < NUM_RECORDS; i++) {
            String key = "key" + String.format("%02d", i);
            assertTrue(filter.mightContain(key));
        }
        assertFalse(filter.mightContain("non-existent-key"));
        assertEquals(avroSchema, hoodieReader.getSchema());
        assertEquals(NUM_RECORDS, hoodieReader.getTotalRecords());
        String[] minMaxRecordKeys = hoodieReader.readMinMaxRecordKeys();
        assertEquals(2, minMaxRecordKeys.length);
        assertEquals("key00", minMaxRecordKeys[0]);
        assertEquals("key" + (NUM_RECORDS - 1), minMaxRecordKeys[1]);
    }

}
