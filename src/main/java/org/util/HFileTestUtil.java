package org.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HFileTestUtil {
    /**
     * Create an HFile with the given number of rows between a given
     * start key and end key @ family:qualifier.  The value will be the key value.
     * This file will not have tags.
     */
    public static void createHFile(
            Configuration configuration,
            FileSystem fs, Path path,
            byte[] family, byte[] qualifier,
            byte[] startKey, byte[] endKey, int numRows) throws IOException {
        createHFile(configuration, fs, path, DataBlockEncoding.NONE, family, qualifier,
                startKey, endKey, numRows, false);
    }

    /**
     * Create an HFile with the given number of rows between a given
     * start key and end key @ family:qualifier.  The value will be the key value.
     * This file will use certain data block encoding algorithm.
     */
    public static void createHFileWithDataBlockEncoding(
            Configuration configuration,
            FileSystem fs, Path path, DataBlockEncoding encoding,
            byte[] family, byte[] qualifier,
            byte[] startKey, byte[] endKey, int numRows) throws IOException {
        createHFile(configuration, fs, path, encoding, family, qualifier, startKey, endKey,
                numRows, false);
    }

    /**
     * Create an HFile with the given number of rows between a given
     * start key and end key @ family:qualifier.  The value will be the key value.
     * This cells will also have a tag whose value is the key.
     */
    public static void createHFileWithTags(
            Configuration configuration,
            FileSystem fs, Path path,
            byte[] family, byte[] qualifier,
            byte[] startKey, byte[] endKey, int numRows) throws IOException {
        createHFile(configuration, fs, path, DataBlockEncoding.NONE, family, qualifier,
                startKey, endKey, numRows, true);
    }

    /**
     * Create an HFile with the given number of rows between a given
     * start key and end key @ family:qualifier.
     * If withTag is true, we add the rowKey as the tag value for
     * tagtype ACL_TAG_TYPE
     */
    public static void createHFile(
            Configuration configuration,
            FileSystem fs, Path path, DataBlockEncoding encoding,
            byte[] family, byte[] qualifier,
            byte[] startKey, byte[] endKey, int numRows, boolean withTag) throws IOException {
        HFileContext meta = new HFileContextBuilder()
                .withIncludesTags(withTag)
                .withDataBlockEncoding(encoding)
                .build();
        HFile.Writer writer = HFile.getWriterFactory(configuration, new CacheConfig(configuration))
                .withPath(fs, path)
                .withFileContext(meta)
                .create();
        long now = System.currentTimeMillis();
        try {
            // subtract 2 since iterateOnSplits doesn't include boundary keys
            for (byte[] key : Bytes.iterateOnSplits(startKey, endKey, numRows - 2)) {
                KeyValue kv = new KeyValue(key, family, qualifier, now, key);
                if (withTag) {
                    // add a tag.  Arbitrarily chose mob tag since we have a helper already.
                    List<Tag> tags = new ArrayList<Tag>();
                    tags.add(new ArrayBackedTag(TagType.ACL_TAG_TYPE, key));
                    kv = new KeyValue(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(),
                            kv.getFamilyArray(), kv.getFamilyOffset(), kv.getFamilyLength(),
                            kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength(),
                            kv.getTimestamp(), KeyValue.Type.Put, kv.getValueArray(), kv.getValueOffset(),
                            kv.getValueLength(), tags);

                    // verify that the kv has the tag.
//                    byte[] ta = kv.getTagsArray();
//                    int toff = kv.getTagsOffset();
//                    int tlen = kv.getTagsLength();
//                    Tag t = Tag.getTag(ta, toff, tlen, TagType.ACL_TAG_TYPE);
//                    if (t == null) {
//                        throw new IllegalStateException("Tag didn't stick to KV " + kv.toString());
//                    }
                }
                writer.append(kv);
            }
        } finally {
            writer.appendFileInfo(HStoreFile.BULKLOAD_TIME_KEY,
                    Bytes.toBytes(System.currentTimeMillis()));
            writer.close();
        }
    }

    /**
     * This verifies that each cell has a tag that is equal to its rowkey name.  For this to work
     * the hbase instance must have HConstants.RPC_CODEC_CONF_KEY set to
     * KeyValueCodecWithTags.class.getCanonicalName());
     * @param table table containing tagged cells
     * @throws IOException if problems reading table
     */
//    public static void verifyTags(Table table) throws IOException {
//        ResultScanner s = table.getScanner(new Scan());
//        for (Result r : s) {
//            for (Cell c : r.listCells()) {
//                byte[] ta = c.getTagsArray();
//                int toff = c.getTagsOffset();
//                int tlen = c.getTagsLength();
//                Tag t = Tag.getTag(ta, toff, tlen, TagType.ACL_TAG_TYPE);
//                if (t == null) {
//                    fail(c.toString() + " has null tag");
//                    continue;
//                }
//                byte[] tval = t.getValue();
//                assertArrayEquals(c.toString() + " has tag" + Bytes.toString(tval),
//                        r.getRow(), tval);
//            }
//        }
//    }
}
