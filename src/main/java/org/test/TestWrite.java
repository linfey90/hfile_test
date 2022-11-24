/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.io.hfile.*;
import org.apache.hadoop.hbase.io.hfile.HFile.Reader;
import org.apache.hadoop.hbase.io.hfile.HFile.Writer;
import org.apache.hadoop.hbase.io.hfile.ReaderContext.ReaderType;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Random;

import static org.apache.hadoop.hbase.io.ByteBuffAllocator.*;

/**
 * test hfile features.
 */
public class TestWrite {


  private static final int NUM_VALID_KEY_TYPES = Type.values().length - 2;
  private static String localFormatter = "%010d";
  private static CacheConfig cacheConf;
  private static Configuration conf;
  private static FileSystem fs;


  public static Reader createReaderFromStream(ReaderContext context, CacheConfig cacheConf,
                                              Configuration conf) throws IOException {
    HFileInfo fileInfo = new HFileInfo(context, conf);
    Reader preadReader = HFile.createReader(context, fileInfo, cacheConf, conf);
    fileInfo.initMetaAndIndex(preadReader);
    preadReader.close();
    context = new ReaderContextBuilder()
      .withFileSystemAndPath(context.getFileSystem(), context.getFilePath())
      .withReaderType(ReaderType.STREAM).build();
    Reader streamReader = HFile.createReader(context, fileInfo, cacheConf, conf);
    return streamReader;
  }

  private ByteBuffAllocator initAllocator(boolean reservoirEnabled, int bufSize, int bufCount,
    int minAllocSize) {
    Configuration that = HBaseConfiguration.create(conf);
    that.setInt(BUFFER_SIZE_KEY, bufSize);
    that.setInt(MAX_BUFFER_COUNT_KEY, bufCount);
    // All ByteBuffers will be allocated from the buffers.
    that.setInt(MIN_ALLOCATE_SIZE_KEY, minAllocSize);
    return ByteBuffAllocator.create(that, reservoirEnabled);
  }



  private void readStoreFile(Path storeFilePath, Configuration conf, ByteBuffAllocator alloc)
    throws Exception {
    // Open the file reader with block cache disabled.
    CacheConfig cache = new CacheConfig(conf, null, null, alloc);
    Reader reader = HFile.createReader(fs, storeFilePath, cache, true, conf);
    long offset = 0;
    while (offset < reader.getTrailer().getLoadOnOpenDataOffset()) {
      HFileBlock block = reader.readBlock(offset, -1, false, true, false, true, null, null);
      offset += block.getOnDiskSizeWithHeader();
      block.release(); // return back the ByteBuffer back to allocator.
    }
    reader.close();
  }


  public static Type generateKeyType(Random rand) {
    if (rand.nextBoolean()) {
      // Let's make half of KVs puts.
      return Type.Put;
    } else {
      Type keyType = Type.values()[1 + rand.nextInt(NUM_VALID_KEY_TYPES)];
      if (keyType == Type.Minimum || keyType == Type.Maximum) {
        throw new RuntimeException("Generated an invalid key type: " + keyType + ". "
          + "Probably the layout of KeyValue.Type has changed.");
      }
      return keyType;
    }
  }

  public static void truncateFile(FileSystem fs, Path src, Path dst) throws IOException {
    FileStatus fst = fs.getFileStatus(src);
    long len = fst.getLen();
    len = len / 2;

    // create a truncated hfile
    FSDataOutputStream fdos = fs.create(dst);
    byte[] buf = new byte[(int) len];
    FSDataInputStream fdis = fs.open(src);
    fdis.read(buf);
    fdos.write(buf);
    fdis.close();
    fdos.close();
  }

  // write some records into the hfile
  // write them twice
  private int writeSomeRecords(Writer writer, int start, int n, boolean useTags)
    throws IOException {
    String value = "value";
    KeyValue kv;
    for (int i = start; i < (start + n); i++) {
      String key = String.format(localFormatter, Integer.valueOf(i));
      if (useTags) {
        Tag t = new ArrayBackedTag((byte) 1, "myTag1");
        Tag[] tags = new Tag[1];
        tags[0] = t;
        kv = new KeyValue(Bytes.toBytes(key), Bytes.toBytes("family"), Bytes.toBytes("qual"),
          HConstants.LATEST_TIMESTAMP, Bytes.toBytes(value + key), tags);
        writer.append(kv);
      } else {
        kv = new KeyValue(Bytes.toBytes(key), Bytes.toBytes("family"), Bytes.toBytes("qual"),
          Bytes.toBytes(value + key));
        writer.append(kv);
      }
    }
    return (start + n);
  }

  private void readAllRecords(HFileScanner scanner) throws IOException {
    readAndCheckbytes(scanner, 0, 100);
  }

  // read the records and check
  private int readAndCheckbytes(HFileScanner scanner, int start, int n) throws IOException {
    String value = "value";
    int i = start;
    for (; i < (start + n); i++) {
      ByteBuffer key = ByteBuffer.wrap(((KeyValue) scanner.getKey()).getKey());
      ByteBuffer val = scanner.getValue();
      String keyStr = String.format(localFormatter, Integer.valueOf(i));
      String valStr = value + keyStr;
      KeyValue kv = new KeyValue(Bytes.toBytes(keyStr), Bytes.toBytes("family"),
        Bytes.toBytes("qual"), Bytes.toBytes(valStr));
      byte[] keyBytes =
        new KeyValue.KeyOnlyKeyValue(Bytes.toBytes(key), 0, Bytes.toBytes(key).length).getKey();
      byte[] valBytes = Bytes.toBytes(val);
      System.out.println("---value:"+ Bytes.toString(valBytes));
      if (!scanner.next()) {
        break;
      }
    }
    return (start + n);
  }

  private byte[] getSomeKey(int rowId) {
    String key = String.format(localFormatter, Integer.valueOf(rowId));
    key = new User(rowId, key).getKey();
    System.out.println("-----key:"+key);
    KeyValue kv = new KeyValue(Bytes.toBytes(key),
      Bytes.toBytes("family"), Bytes.toBytes("qual"), HConstants.LATEST_TIMESTAMP, Type.Put);
    return kv.getKey();
  }

  private void writeRecords(Writer writer, boolean useTags) throws IOException {
    writeSomeRecords(writer, 0, 100, useTags);
    writer.close();
  }

  private static FSDataOutputStream createFSOutput(Path name) throws IOException {
    // if (fs.exists(name)) fs.delete(name, true);
    FSDataOutputStream fout = fs.create(name);
    return fout;
  }

//  private static String filePath = "/user/data/hfile2/none_1y";
//  private static int count = 100000000;

  //java -jar HfileTest-1.0-SNAPSHOT-shaded.jar gz /user/data/hfile2/gz_1kw 10000000
  public static void main(String[] args) throws Exception {
    if(args.length != 3){
      System.out.println("error,parameters has three");
      System.exit(1);
    }
    String codec = args[0];
    boolean useTags = false;
    String filePath = args[1];
    int count = Integer.parseInt(args[2]);
    if (useTags) {
      conf.setInt("hfile.format.version", 3);
    }
    conf = new Configuration();
    cacheConf = new CacheConfig(conf);
    // 这个解决hdfs问题
    conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    // 这个解决本地file问题
    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    System.setProperty("HADOOP_USER_NAME", "root");
    conf.set("fs.default.name", "hdfs://node04:8020");
    fs = FileSystem.get(conf);

    Path ncHFile = new Path(filePath);
    FSDataOutputStream fout = createFSOutput(ncHFile);
    HFileContext meta = new HFileContextBuilder().withBlockSize(512)
      .withCompression(HFileWriterImpl.compressionByName(codec)).build();
    Writer writer =
      HFile.getWriterFactory(conf, cacheConf).withOutputStream(fout).withFileContext(meta).create();
    System.out.println(Objects.toString(writer));
    long start = System.nanoTime();
    writeRecords2(writer, useTags, count);
    System.out.println("---write cost:"+ DateTimeUtils.getCostS(start)+" s");
    fout.close();
  }

  private static void writeRecords2(Writer writer, boolean useTags, int total) throws IOException {
    String value = "value";
    KeyValue kv;
    for (int i = 0; i < total; i++) {
      if(i%100000 == 0){
        System.out.println("批次:"+ i/100000);
      }
      String key = String.format(localFormatter, Integer.valueOf(i));
      User user = new User(i, key);
      key = user.getKey();
      if (useTags) {
        Tag t = new ArrayBackedTag((byte) 1, "myTag1");
        Tag[] tags = new Tag[1];
        tags[0] = t;
        kv = new KeyValue(Bytes.toBytes(key), Bytes.toBytes("family"), Bytes.toBytes("qual"),
          HConstants.LATEST_TIMESTAMP, Bytes.toBytes(value + key), tags);
        writer.append(kv);
      } else {
        kv = new KeyValue(Bytes.toBytes(key), Bytes.toBytes("family"), Bytes.toBytes("qual"),
          Bytes.toBytes(user.toString()));
        writer.append(kv);
      }
    }
    writer.close();
  }


}
