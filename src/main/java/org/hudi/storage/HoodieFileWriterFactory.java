/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hudi.storage;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.hudi.bloom.BloomFilter;
import org.hudi.bloom.BloomFilterFactory;
import org.util.TaskContextSupplier;

import java.io.IOException;
import static org.hudi.storage.HoodieHFileConfig.CACHE_DATA_IN_L1;
import static org.hudi.storage.HoodieHFileConfig.DROP_BEHIND_CACHE_COMPACTION;
import static org.hudi.storage.HoodieHFileConfig.HFILE_COMPARATOR;
import static org.hudi.storage.HoodieHFileConfig.PREFETCH_ON_OPEN;

public class HoodieFileWriterFactory {

//  public static <T extends HoodieRecordPayload, R extends IndexedRecord, I, K, O> HoodieFileWriter<R> getFileWriter(
//      String instantTime, Path path, HoodieTable<T, I, K, O> hoodieTable, HoodieWriteConfig config, Schema schema,
//      TaskContextSupplier taskContextSupplier) throws IOException {
//    final String extension = FSUtils.getFileExtension(path.getName());
//    if (PARQUET.getFileExtension().equals(extension)) {
//      return newParquetFileWriter(instantTime, path, config, schema, hoodieTable, taskContextSupplier, config.populateMetaFields());
//    }
//    if (HFILE.getFileExtension().equals(extension)) {
//      return newHFileFileWriter(
//          instantTime, path, config, schema, hoodieTable.getHadoopConf(), taskContextSupplier);
//    }
//    if (ORC.getFileExtension().equals(extension)) {
//      return newOrcFileWriter(
//          instantTime, path, config, schema, hoodieTable.getHadoopConf(), taskContextSupplier);
//    }
//    throw new UnsupportedOperationException(extension + " format not supported yet.");
//  }

//  private static <T extends HoodieRecordPayload, R extends IndexedRecord> HoodieFileWriter<R> newParquetFileWriter(
//      String instantTime, Path path, HoodieWriteConfig config, Schema schema, HoodieTable hoodieTable,
//      TaskContextSupplier taskContextSupplier, boolean populateMetaFields) throws IOException {
//    return newParquetFileWriter(instantTime, path, config, schema, hoodieTable.getHadoopConf(),
//        taskContextSupplier, populateMetaFields, populateMetaFields);
//  }

//  private static <T extends HoodieRecordPayload, R extends IndexedRecord> HoodieFileWriter<R> newParquetFileWriter(
//      String instantTime, Path path, HoodieWriteConfig config, Schema schema, Configuration conf,
//      TaskContextSupplier taskContextSupplier, boolean populateMetaFields, boolean enableBloomFilter) throws IOException {
//    Option<BloomFilter> filter = enableBloomFilter ? Option.of(createBloomFilter(config)) : Option.empty();
//    HoodieAvroWriteSupport writeSupport = new HoodieAvroWriteSupport(new AvroSchemaConverter(conf).convert(schema), schema, filter);
//
//    HoodieParquetConfig<HoodieAvroWriteSupport> parquetConfig = new HoodieParquetConfig<>(writeSupport, config.getParquetCompressionCodec(),
//        config.getParquetBlockSize(), config.getParquetPageSize(), config.getParquetMaxFileSize(),
//        conf, config.getParquetCompressionRatio(), config.parquetDictionaryEnabled());
//
//    return new HoodieAvroParquetWriter<>(path, parquetConfig, instantTime, taskContextSupplier, populateMetaFields);
//  }

  public static <T extends HoodieRecordPayload, R extends IndexedRecord> HoodieFileWriter<R> newHFileFileWriter(
          String instantTime, Path path, Schema schema, Configuration conf,
          TaskContextSupplier taskContextSupplier, boolean populateMetaFields) throws IOException {

    BloomFilter filter = createBloomFilter();
    HoodieHFileConfig hfileConfig = new HoodieHFileConfig(conf,
            Compression.Algorithm.valueOf("GZ"), 1024 * 1024, 120 * 1024 * 1024,
        HoodieHFileReader.KEY_FIELD_NAME, PREFETCH_ON_OPEN, CACHE_DATA_IN_L1, DROP_BEHIND_CACHE_COMPACTION,
        filter, HFILE_COMPARATOR);

    return new HoodieHFileWriter<>(instantTime, path, hfileConfig, schema, taskContextSupplier, populateMetaFields);
  }

//  private static <T extends HoodieRecordPayload, R extends IndexedRecord> HoodieFileWriter<R> newOrcFileWriter(
//      String instantTime, Path path, HoodieWriteConfig config, Schema schema, Configuration conf,
//      TaskContextSupplier taskContextSupplier) throws IOException {
//    BloomFilter filter = createBloomFilter(config);
//    HoodieOrcConfig orcConfig = new HoodieOrcConfig(conf, config.getOrcCompressionCodec(),
//        config.getOrcStripeSize(), config.getOrcBlockSize(), config.getOrcMaxFileSize(), filter);
//    return new HoodieOrcWriter<>(instantTime, path, orcConfig, schema, taskContextSupplier);
//  }

  private static BloomFilter createBloomFilter() {
    return BloomFilterFactory.createBloomFilter(5000000, 0.00001, 10000000,
            "DYNAMIC_V0");
  }
}
