package org.hudi;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.hudi.bloom.BloomFilter;
import org.hudi.mine.StoreConfig;
import org.hudi.storage.HoodieHFileReader;
import org.junit.Test;
import org.test.DateTimeUtils;
import org.test.User;
import org.util.Option;

import java.util.*;

public class TestRead {
    public static void main(String[] args) throws Exception{
        int parameters = 3;
        if(args.length != parameters){
            System.out.println("error,parameters has " + parameters);
            System.exit(1);
        }
        boolean bloomRead = Boolean.parseBoolean(args[0]);
        boolean actualRead = Boolean.parseBoolean(args[1]);
        int fileCount = Integer.parseInt(args[2]);
        int total = 1000000;
        int batch = 10000;
//        int total = 100;
//        int batch = 10;
//        int fileCount = 10;
        String path = "/user/data/files/";
        String dataPath = path + "t_user_" + total/batch + "w";

        Map<Integer, BloomFilter> bloomMap = new HashMap();
        Map<Integer, HoodieHFileReader> readerMap = new HashMap();
        Path indexPath;
        for(int i=1;i<=fileCount; i++){
            indexPath = new Path( dataPath + i+".hfile");
            HoodieHFileReader hoodieHFileReader = (HoodieHFileReader) TestBloom.createReader(indexPath);
            //bloom filter
            BloomFilter filter = hoodieHFileReader.readBloomFilter();
            bloomMap.computeIfAbsent(i, k -> filter);
            readerMap.computeIfAbsent(i, k -> hoodieHFileReader);
        }
        HoodieHFileReader hFileReader;
        GroupReadSupport readSupport = new GroupReadSupport();
        ParquetReader.Builder reader= ParquetReader
                .builder(readSupport, new Path(dataPath))
                .withConf(StoreConfig.getConf());
        ParquetReader build = reader.build();
        Group line = null;
        int count = 0, nullCount = 0, mightContain = 0, actualContain = 0;
        long times = 0;
        long start = System.nanoTime();
        while((line= (Group) build.read())!=null){
            count++;
            if (!bloomRead) continue;
            //通过下标和字段名称都可以获取
            Integer id = line.getInteger("id", 0);
            String rowkey = User.getKey(id);
            boolean isExist = false;
            for(int i=1;i <= fileCount; i++){
                BloomFilter filter = bloomMap.get(i);
                if (filter.mightContain(rowkey)) {
                    mightContain++;
                    isExist = true;
                    if (actualRead) {
                        hFileReader = readerMap.get(i);
                        long st = System.nanoTime();
                        Option recordByKey = hFileReader.getRecordByKey(rowkey);
                        times += (System.nanoTime()-st);
//                        Option recordByKey = hFileReader.fetchRecordByKeyInternal(hFileReader. , rowkey, hFileReader.getSchema(), hFileReader.getSchema());
                        if (recordByKey.isPresent()){
                            actualContain++;
                            break;
                        }
                    } else {
                        break;
                    }
                }
            }
            if(!isExist) nullCount++;
        }
        System.out.println("key find cost:"+ (times/1000000) +"ms, key:"+(times/1000000d/mightContain)+"ms");
        System.out.println("读取结束,count:"+count+",nullCount:"+nullCount+",mightContain:"+mightContain+",actualContain:"+actualContain+",cost:"+DateTimeUtils.getCostS(start)+"s");
    }

    @Test
    public void readCache() throws Exception{
        String path = "/user/data/files/t_user_100w1.hfile";
        Path indexPath = new Path(path);
        String rowkey = User.getKey(1);
        HoodieHFileReader hFileReader = (HoodieHFileReader) TestBloom.createReaderWithCache(indexPath);
        Option recordByKey = hFileReader.getRecordByKey(rowkey);
        if(recordByKey.isPresent()){
            System.out.println(recordByKey.get());
        }
        recordByKey = hFileReader.getRecordByKey(User.getKey(4000000));
        if(recordByKey.isPresent()){
            System.out.println(recordByKey.get());
        }

        recordByKey = hFileReader.getRecordByKey(User.getKey(2));
        if(recordByKey.isPresent()){
            System.out.println(recordByKey.get());
        }
    }

    @Test
    public void batchRead() throws Exception {

        String path = "/user/data/files/t_user_100w1.hfile";
        Path indexPath = new Path(path);
        HoodieHFileReader hFileReader = (HoodieHFileReader) TestBloom.createReaderWithCache(indexPath);
        long start = System.nanoTime();
        List<String> rowkeys = new ArrayList<>();
        String rowkey2 = User.getKey(20);
        rowkeys.add(rowkey2);
        String rowkey = User.getKey(6000000);
        rowkeys.add(rowkey);
        String rowkey1 = User.getKey(1);
        rowkeys.add(rowkey1);
        String rowkey3 = User.getKey(300000);
        rowkeys.add(rowkey3);
        Collections.sort(rowkeys);
        Set<String> set = hFileReader.filterRowKeys(rowkeys);
        System.out.println("cost:"+DateTimeUtils.getCostMs(start)+"ms");
        set.forEach(System.out::println);
    }

    @Test
    public void test() throws Exception {
        String str = "0000000004name0000000002asdasdfasdfadfa223343434address2zhejiangzhejiang阿里巴巴集团";
        String min = "0000000003name0000000003asdasdfasdfadfa223343434address3zhejiangzhejiang阿里巴巴集团";
        String max = "0000000020name0000000020asdasdfasdfadfa223343434address20zhejiangzhejiang阿里巴巴集团";
        System.out.println(str.compareTo(min));//+ ：str>min
        System.out.println(str.compareTo(max));//- : str<max
        if(str.compareTo(min)>=0 && str.compareTo(max)<=0){
            System.out.println("cotain");
        }

    }
}
