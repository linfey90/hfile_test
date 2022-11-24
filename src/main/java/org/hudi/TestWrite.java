package org.hudi;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetWriter;
import org.hudi.storage.HoodieFileWriter;
import org.test.DateTimeUtils;
import org.test.User;
import org.util.SchemaTestUtil;

public class TestWrite {
    public static void main(String[] args) throws Exception{
        int total = 1000000;
        int batch = 10000;
        int hcount = 500*10000;
//        int total = 100;
//        int batch = 10;
//        int hcount = 500;
        String path = "/user/data/files/";
        String dataPath = path + "t_user_" + total/batch + "w";

        long start = System.nanoTime();
        Path indexPath = new Path( dataPath + "1.hfile");
        boolean populateMetaFields= false;
        Schema avroSchema = SchemaTestUtil.getSchemaFromResource(TestBloom.class, "/exampleSchemaWithMetaFields.avsc");
        HoodieFileWriter<GenericRecord> idxWriter = TestBloom.createWriter(avroSchema, indexPath, populateMetaFields);

        ParquetWriter<Group> writer = ParquetTest.getWriter(dataPath);
        for(int i=0; i < total; i++) {
            //写数据
            User user = new User(i);
            ParquetTest.writeUser(writer, user);

            int num = (i+1)/batch;
            //写 hfile
            if(num <= 20) {
                GenericRecord record = new GenericData.Record(avroSchema);
                record.put("id", user.getId()+"");
                record.put("name", user.getName());
                record.put("path", user.getAddress());
                idxWriter.writeAvro(user.getKey(i), record);
                //切换文件
                if((i+1)%batch == 0) {
                    System.out.println("数据完成批次:"+ num);
                    //最后一条结束
                    if(i+1 == total) return;
                    //hfile超出范围的基数=total
                    int hmax = hcount + (num-1)*batch + total;
                    //写hfile
                    for(int j=total+i+1;j < hmax;j++) {
                        user = new User(j);
                        record = new GenericData.Record(avroSchema);
                        record.put("id", user.getId()+"");
                        record.put("name", user.getName());
                        record.put("path", user.getAddress());
                        idxWriter.writeAvro(user.getKey(i), record);
                    }

                    idxWriter.close();
                    indexPath = new Path( dataPath + (num+1) + ".hfile");
                    idxWriter = TestBloom.createWriter(avroSchema, indexPath, populateMetaFields);
                }
            }
        }
        idxWriter.close();
        ParquetTest.close(writer);
        System.out.println("write success, cost:"+ DateTimeUtils.getCostS(start)+"s");
    }
}
