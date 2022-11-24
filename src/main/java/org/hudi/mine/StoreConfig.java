package org.hudi.mine;

import org.apache.hadoop.conf.Configuration;

public class StoreConfig {
    public static Configuration getConf(){
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration conf = new Configuration();
        // 这个解决hdfs问题
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        // 这个解决本地file问题
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        conf.set("fs.default.name", "hdfs://node10:8020");
//        conf.set("fs.default.name", "hdfs://node04:8020");
        return conf;
    }
}
