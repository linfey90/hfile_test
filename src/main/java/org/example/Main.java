package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.hfile.*;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.security.Key;
import java.util.Random;

public class Main {
    public static int BLOCKSIZE = 64 * 1024;
    public static Compression.Algorithm COMPRESSION = Compression.Algorithm.NONE;

    private final static Configuration conf = new Configuration();
    private static FileSystem fs = null;
    private final static int NUM_CFS = 10;

    private final static String hdfsPathStr = "hdfs://10.0.10.13:8020/user/HfileTest/";
    private final static String pathStr = "/home/bigdata/jars/Hfile.bin";

    private final static byte[] QUAL = Bytes.toBytes("qual");

    public static void main(String[] args) throws IOException {
        fs = HFileSystem.get(conf);
        Path filePath = new Path(pathStr);
        byte[] fam = Bytes.toBytes("family01");
        byte[] val = Bytes.toBytes(String.format("%010d", 100));
        createHFile(fs, filePath, fam, QUAL, val, 1000);
    }

    public static void createHFile(FileSystem fs, Path path, byte[] family,
                                   byte[] qualifier, byte[] value, int numRows) throws IOException {
        HFileContext context = new HFileContextBuilder().withBlockSize(BLOCKSIZE)
                .withCompression(COMPRESSION)
                .build();
        HFile.Writer writer = HFile
                .getWriterFactory(conf, new CacheConfig(conf))
                .withPath(fs, path)
                .withFileContext(context)
                .create();
        long now = System.currentTimeMillis();
        try {
            // subtract 2 since iterateOnSplits doesn't include boundary keys
            for (int i = 0; i < numRows; i++) {
                KeyValue kv = new KeyValue(rowkey(i), family, qualifier, now, value);
                writer.append(kv);
            }
            writer.appendFileInfo(HStoreFile.BULKLOAD_TIME_KEY, Bytes.toBytes(now));
        } finally {
            writer.close();
        }
    }
    /**
     * Create a rowkey compatible with
     * {@link #createHFile(FileSystem, Path, byte[], byte[], byte[], int)}.
     */
    public static byte[] rowkey(int i) {
        return Bytes.toBytes(String.format("row_%08d", i));
    }

//    private static void readHFileRandom(Path file, Compression.Algorithm compression) throws Exception
//    {
//        CacheConfig cacheConf = new CacheConfig(conf);
//        HFile.Reader reader = HFile.createReader(hdfs, file, cacheConf, conf);
//        HFileScanner scanner = reader.getScanner(true, true, false);
//
//        @SuppressWarnings("unused")
//        Cell kv = null;
//        scanner.seekTo();
//        Random random = new Random();
//        for (int i = 0; i < 1000; i++) {
//            scanner.seekTo();
//            scanner.seekTo(getKey(random.nextInt(testSize)).getBytes());
//            kv = scanner.getCell();
//            //logger.debug("key: {} value: {}", new String (kv.getKey()), new String (kv.getValue()));
//        }
//    }

    private byte[] extractHFileKey(Path path) throws Exception {
        HFile.Reader reader = HFile.createReader(fs, path,conf);
        try {
//            reader.loadFileInfo();
            Encryption.Context cryptoContext = reader.getFileContext().getEncryptionContext();
//            assertNotNull("Reader has a null crypto context", cryptoContext);
            Key key = cryptoContext.getKey();
//            assertNotNull("Crypto context has no key", key);
            if (key == null) {
                return null;
            }
            return key.getEncoded();
        } finally {
            reader.close();
        }
    }

}