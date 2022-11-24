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

package org.util;

import org.apache.avro.*;
import org.apache.avro.Conversions.DecimalConversion;
import org.apache.avro.LogicalTypes.Decimal;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.*;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.io.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Helper class to do common stuff across Avro.
 */
public class HoodieAvroUtils {
  private static final ThreadLocal<BinaryEncoder> BINARY_ENCODER = ThreadLocal.withInitial(() -> null);
  private static final ThreadLocal<BinaryDecoder> BINARY_DECODER = ThreadLocal.withInitial(() -> null);
  /**
   * Convert serialized bytes back into avro record.
   */
  public static GenericRecord bytesToAvro(byte[] bytes, Schema writerSchema, Schema readerSchema) throws IOException {
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, BINARY_DECODER.get());
    BINARY_DECODER.set(decoder);
    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(writerSchema, readerSchema);
    return reader.read(null, decoder);
  }

  /**
   * Convert a given avro record to bytes.
   */
  public static byte[] avroToBytes(GenericRecord record) {
    return indexedRecordToBytes(record);
  }

  public static <T extends IndexedRecord> byte[] indexedRecordToBytes(T record) {
    GenericDatumWriter<T> writer = new GenericDatumWriter<>(record.getSchema(), ConvertingGenericData.INSTANCE);
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, BINARY_ENCODER.get());
      BINARY_ENCODER.set(encoder);
      writer.write(record, encoder);
      encoder.flush();
      return out.toByteArray();
    } catch (IOException e) {
      throw new HoodieIOException("Cannot convert GenericRecord to bytes", e);
    }
  }
  public static final String COMMIT_TIME_METADATA_FIELD = "_hoodie_commit_time";
  public static final String COMMIT_SEQNO_METADATA_FIELD = "_hoodie_commit_seqno";
  public static final String RECORD_KEY_METADATA_FIELD = "_hoodie_record_key";
  public static final String PARTITION_PATH_METADATA_FIELD = "_hoodie_partition_path";
  public static final String FILENAME_METADATA_FIELD = "_hoodie_file_name";
  public static final String OPERATION_METADATA_FIELD = "_hoodie_operation";
  public static final String HOODIE_IS_DELETED = "_hoodie_is_deleted";

  public static GenericRecord addHoodieKeyToRecord(GenericRecord record, String recordKey, String partitionPath,
                                                   String fileName) {
    record.put(FILENAME_METADATA_FIELD, fileName);
    record.put(PARTITION_PATH_METADATA_FIELD, partitionPath);
    record.put(RECORD_KEY_METADATA_FIELD, recordKey);
    return record;
  }
  /**
   * Adds the Hoodie commit metadata into the provided Generic Record.
   */
  public static GenericRecord addCommitMetadataToRecord(GenericRecord record, String instantTime, String commitSeqno) {
    record.put(COMMIT_TIME_METADATA_FIELD, instantTime);
    record.put(COMMIT_SEQNO_METADATA_FIELD, commitSeqno);
    return record;
  }

}
