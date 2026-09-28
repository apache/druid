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

import org.apache.kafka.common.record.TimestampType;

import javax.annotation.Nullable;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.zip.GZIPInputStream;

final class ShareGroupRawBatchReader
{
  private static final int MAX_STRING_BYTES = 1 << 20;
  private static final int MAX_RECORD_BYTES = 128 << 20;
  private static final int MAX_RECORDS = 1_000_000;
  private static final int MAX_HEADERS = 100_000;

  ShareGroupRawBatch read(InputStream inputStream) throws IOException
  {
    final DataInputStream envelope = new DataInputStream(inputStream);
    final int magic = envelope.readInt();
    if (magic != ShareGroupRawBatchWriter.MAGIC) {
      throw new IOException("Invalid share-group raw batch magic");
    }
    final short version = envelope.readShort();
    if (version != ShareGroupRawBatchWriter.VERSION) {
      throw new IOException("Unsupported share-group raw batch version " + version);
    }
    final byte compression = envelope.readByte();
    if (compression != ShareGroupRawBatchWriter.GZIP) {
      throw new IOException("Unsupported share-group raw batch compression " + compression);
    }
    envelope.readByte();

    final MessageDigest digest = ShareGroupRawBatchWriter.newDigest();
    final DigestInputStream digestInput = new DigestInputStream(new GZIPInputStream(envelope), digest);
    final DataInputStream payload = new DataInputStream(digestInput);
    final String topic = readString(payload);
    final int partition = payload.readInt();
    final int recordCount = readSize(payload, MAX_RECORDS, "record count");
    final long expectedFirstOffset = payload.readLong();
    final long expectedLastOffset = payload.readLong();
    final List<ShareGroupRawRecord> records = new ArrayList<>(recordCount);
    for (int i = 0; i < recordCount; i++) {
      records.add(readRecord(payload));
    }

    digestInput.on(false);
    final int digestSize = readSize(payload, ShareGroupRawBatchWriter.DIGEST_SIZE, "digest size");
    if (digestSize != ShareGroupRawBatchWriter.DIGEST_SIZE) {
      throw new IOException("Invalid share-group raw batch digest size " + digestSize);
    }
    final byte[] expectedDigest = new byte[digestSize];
    payload.readFully(expectedDigest);
    if (!MessageDigest.isEqual(expectedDigest, digest.digest())) {
      throw new IOException("Share-group raw batch checksum mismatch");
    }
    if (payload.read() != -1) {
      throw new IOException("Unexpected trailing share-group raw batch data");
    }

    final ShareGroupRawBatch batch = new ShareGroupRawBatch(topic, partition, records);
    if (batch.getFirstOffset() != expectedFirstOffset || batch.getLastOffset() != expectedLastOffset) {
      throw new IOException("Share-group raw batch offset bounds mismatch");
    }
    return batch;
  }

  ShareGroupRawBatch read(InputStream inputStream, String expectedTopic, int expectedPartition) throws IOException
  {
    final ShareGroupRawBatch batch = read(inputStream);
    if (!batch.getTopic().equals(expectedTopic) || batch.getPartition() != expectedPartition) {
      throw new IOException(
          "Share-group raw batch identity mismatch: expected "
          + expectedTopic + ':' + expectedPartition + " but found "
          + batch.getTopic() + ':' + batch.getPartition()
      );
    }
    return batch;
  }

  private static ShareGroupRawRecord readRecord(DataInputStream input) throws IOException
  {
    final long offset = input.readLong();
    final long timestamp = input.readLong();
    final TimestampType timestampType = timestampType(input.readByte());
    final int serializedKeySize = input.readInt();
    final int serializedValueSize = input.readInt();
    final byte[] key = readNullableBytes(input);
    final byte[] value = readNullableBytes(input);
    final int headerCount = readSize(input, MAX_HEADERS, "header count");
    final List<ShareGroupRawRecord.RawHeader> headers = new ArrayList<>(headerCount);
    for (int i = 0; i < headerCount; i++) {
      headers.add(new ShareGroupRawRecord.RawHeader(readString(input), readNullableBytes(input)));
    }
    final Optional<Integer> leaderEpoch = input.readBoolean()
                                          ? Optional.of(input.readInt())
                                          : Optional.empty();
    final Optional<Short> deliveryCount = input.readBoolean()
                                          ? Optional.of(input.readShort())
                                          : Optional.empty();
    return new ShareGroupRawRecord(
        offset,
        timestamp,
        timestampType,
        serializedKeySize,
        serializedValueSize,
        key,
        value,
        headers,
        leaderEpoch,
        deliveryCount
    );
  }

  private static String readString(DataInputStream input) throws IOException
  {
    final int size = readSize(input, MAX_STRING_BYTES, "string length");
    final byte[] bytes = new byte[size];
    input.readFully(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  @Nullable
  private static byte[] readNullableBytes(DataInputStream input) throws IOException
  {
    final int size = input.readInt();
    if (size == -1) {
      return null;
    }
    if (size < 0 || size > MAX_RECORD_BYTES) {
      throw new IOException("Invalid record field length " + size);
    }
    final byte[] bytes = new byte[size];
    input.readFully(bytes);
    return bytes;
  }

  private static int readSize(DataInputStream input, int max, String name) throws IOException
  {
    final int size = input.readInt();
    if (size < 0 || size > max) {
      throw new IOException("Invalid " + name + ' ' + size);
    }
    return size;
  }

  private static TimestampType timestampType(byte id) throws IOException
  {
    switch (id) {
      case -1:
        return TimestampType.NO_TIMESTAMP_TYPE;
      case 0:
        return TimestampType.CREATE_TIME;
      case 1:
        return TimestampType.LOG_APPEND_TIME;
      default:
        throw new IOException("Unsupported Kafka timestamp type " + id);
    }
  }
}
