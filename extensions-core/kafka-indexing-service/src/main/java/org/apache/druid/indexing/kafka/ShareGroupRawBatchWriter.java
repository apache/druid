/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexing.kafka;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.zip.GZIPOutputStream;

final class ShareGroupRawBatchWriter
{
  static final int MAGIC = 0x44525347;
  static final short VERSION = 1;
  static final byte GZIP = 1;
  static final int DIGEST_SIZE = 32;

  void write(ShareGroupRawBatch batch, OutputStream outputStream) throws IOException
  {
    final DataOutputStream envelope = new DataOutputStream(outputStream);
    envelope.writeInt(MAGIC);
    envelope.writeShort(VERSION);
    envelope.writeByte(GZIP);
    envelope.writeByte(0);

    final GZIPOutputStream gzip = new GZIPOutputStream(envelope);
    final DigestOutputStream digestOutput = new DigestOutputStream(gzip, newDigest());
    final DataOutputStream payload = new DataOutputStream(digestOutput);
    writeString(payload, batch.getTopic());
    payload.writeInt(batch.getPartition());
    payload.writeInt(batch.getRecords().size());
    payload.writeLong(batch.getFirstOffset());
    payload.writeLong(batch.getLastOffset());
    for (ShareGroupRawRecord record : batch.getRecords()) {
      writeRecord(payload, record);
    }
    payload.flush();

    digestOutput.on(false);
    final byte[] digest = digestOutput.getMessageDigest().digest();
    payload.writeInt(digest.length);
    payload.write(digest);
    payload.flush();
    gzip.finish();
    envelope.flush();
  }

  private static void writeRecord(DataOutputStream output, ShareGroupRawRecord record) throws IOException
  {
    output.writeLong(record.getOffset());
    output.writeLong(record.getTimestamp());
    output.writeByte(record.getTimestampType().id);
    output.writeInt(record.getSerializedKeySize());
    output.writeInt(record.getSerializedValueSize());
    writeNullableBytes(output, record.getKey());
    writeNullableBytes(output, record.getValue());
    output.writeInt(record.getHeaders().size());
    for (ShareGroupRawRecord.RawHeader header : record.getHeaders()) {
      writeString(output, header.getKey());
      writeNullableBytes(output, header.getValue());
    }
    output.writeBoolean(record.getLeaderEpoch().isPresent());
    if (record.getLeaderEpoch().isPresent()) {
      output.writeInt(record.getLeaderEpoch().get());
    }
    output.writeBoolean(record.getDeliveryCount().isPresent());
    if (record.getDeliveryCount().isPresent()) {
      output.writeShort(record.getDeliveryCount().get());
    }
  }

  private static void writeString(DataOutputStream output, String value) throws IOException
  {
    final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    output.writeInt(bytes.length);
    output.write(bytes);
  }

  private static void writeNullableBytes(DataOutputStream output, byte[] value) throws IOException
  {
    if (value == null) {
      output.writeInt(-1);
    } else {
      output.writeInt(value.length);
      output.write(value);
    }
  }

  static MessageDigest newDigest()
  {
    try {
      return MessageDigest.getInstance("SHA-256");
    }
    catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }
}
