package org.hudi;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.hudi.mine.StoreConfig;
import org.test.DateTimeUtils;
import org.test.User;

public class ParquetTest {
    public static String localFormatter = "%010d";

    public static void main(String[] args) throws Exception {
        // 设置执行HDFS操作的用户，防止权限不够
        String filepath = "/user/data/files/t_user_100w";
//        createFile(filepath, 100);
        parquetReaderV2(filepath);
    }

    public static final MessageType FILE_SCHEMA = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32).named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("name")
            .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("address")
            .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("city")
            .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("company")
            .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("path")
            .named("test");

    public static final SimpleGroupFactory f = new SimpleGroupFactory(FILE_SCHEMA);

    public static ParquetWriter getWriter(String filepath) throws Exception{
        Path path = new Path(filepath);
        FileSystem fs = path.getFileSystem(StoreConfig.getConf());
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
        ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
                .withConf(StoreConfig.getConf())
                .withType(FILE_SCHEMA)
                .build();
        return writer;
    }

    public static void close(ParquetWriter writer) throws Exception{
        writer.close();
    }

    public static void writeUser(ParquetWriter<Group> writer, User user) throws Exception {
        Group group = f.newGroup();
        group.add("id", user.getId());
        group.add("name", user.getName());
        group.add("address", user.getAddress());
        group.add("city", user.getCity());
        group.add("company", user.getCompany());
        group.add("path", user.getPath());
        writer.write(group);
    }

    public static void createFile(String filepath, int total) throws Exception {
        ParquetWriter<Group> writer = getWriter(filepath);
        for(int i=1; i <= total;i++){
            String key = String.format(localFormatter, Integer.valueOf(i));
            User user = new User(i, key);
            writeUser(writer, user);
            if(i%100000 == 0){
                System.out.println("批次:"+ i/100000);
            }
        }
        writer.close();
        System.out.println("write over success");
    }

    public static void parquetReaderV2(String inPath) throws Exception{
        GroupReadSupport readSupport = new GroupReadSupport();
        ParquetReader.Builder reader= ParquetReader
                .builder(readSupport, new Path(inPath))
                .withConf(StoreConfig.getConf());
        ParquetReader build = reader.build();
        Group line=null;
        int count = 0;
        long start = System.nanoTime();
        while((line= (Group) build.read())!=null){
            count++;
            //通过下标和字段名称都可以获取
//            System.out.println(line.getInteger("id", 0)+"\t"+
//                    line.getString("name", 0)+"\t"+
//                    line.getString("address", 0)+"\t"+
//                    line.getString("city", 0)+"\t"+
//                    line.getString("company", 0)+"\t");
//                System.out.println(line.toString());
        }
        System.out.println("读取结束,count:"+count+",cost:"+ DateTimeUtils.getCostS(start)+"s");
    }
}
