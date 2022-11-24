package org.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.io.hfile.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

public class MyMulRead implements Runnable{

  private int count = 10000000;
  private String path = "/user/data/hfile/n_1kw2";
  private String localFormatter = "%010d";

  public MyMulRead(String path, int count){
    this.path = path;
    this.count = count;
  }
  @Override public void run() {
    try {
      Configuration conf = new Configuration();
      CacheConfig cacheConf = new CacheConfig(conf);
      //    fs = TEST_UTIL.getTestFileSystem();
      System.setProperty("HADOOP_USER_NAME", "root");
      conf.set("fs.default.name", "hdfs://node04:8020");
      // 这个解决hdfs问题
      conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
      // 这个解决本地file问题
      conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
      FileSystem fs = FileSystem.get(conf);
      Path ncHFile = new Path(path);
      FSDataInputStream fin = fs.open(ncHFile);
      ReaderContext context = new ReaderContextBuilder().withFileSystemAndPath(fs, ncHFile).build();
      HFile.Reader reader = createReaderFromStream(context, cacheConf, conf);
      System.out.println(cacheConf.toString());

      // Load up the index.
      // Get a scanner that caches and that does not use pread.
//      HFileScanner scanner = reader.getScanner(conf, true, false);
      HFileScanner scanner = reader.getScanner(true, false);
      // Align scanner at start of the file.
      scanner.seekTo();
      Random random = new Random();
      int total = 1000;
      float times = 0;
      for(int i=1;i<=total;i++){
        long start = System.nanoTime();
        int data = random.nextInt(count/(i*10));
        System.out.println("---data:"+data);
        scanner.seekTo(KeyValueUtil.createKeyValueFromKey(getSomeKey(data)));
        ByteBuffer val1 = scanner.getValue();
        //    System.out.println("val1:"+ SerializeObjectTool.unserizlize(Bytes.toBytes(val1)).toString());
        //    System.out.println("val2:"+ SerializeObjectTool.unserizlize(Bytes.toBytes(val2)).toString());
        //    assertTrue(Arrays.equals(Bytes.toBytes(val1), Bytes.toBytes(val2)));
        String user = Bytes.toString(Bytes.toBytes(val1));
        System.out.println(user);
        long time = DateTimeUtils.getCostMs(start);
        times += time;
        System.out.println("---read cost:"+time+" ms");
      }
      System.out.println("---read avg cost:"+times/total+" ms");
      reader.close();
      fin.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private byte[] getSomeKey(int rowId) {
    String key = String.format(localFormatter, Integer.valueOf(rowId));
    key = new User(rowId, key).getKey();
    System.out.println("-----key:"+key);
    KeyValue kv = new KeyValue(Bytes.toBytes(key),
      Bytes.toBytes("family"), Bytes.toBytes("qual"), HConstants.LATEST_TIMESTAMP, KeyValue.Type.Put);
    return kv.getKey();
  }

  public HFile.Reader createReaderFromStream(ReaderContext context, CacheConfig cacheConf,
    Configuration conf) throws IOException {
    HFileInfo fileInfo = new HFileInfo(context, conf);
    HFile.Reader preadReader = HFile.createReader(context, fileInfo, cacheConf, conf);
    fileInfo.initMetaAndIndex(preadReader);
    preadReader.close();
    context = new ReaderContextBuilder()
      .withFileSystemAndPath(context.getFileSystem(), context.getFilePath())
      .withReaderType(ReaderContext.ReaderType.STREAM).build();
    HFile.Reader streamReader = HFile.createReader(context, fileInfo, cacheConf, conf);
    return streamReader;
  }
}
