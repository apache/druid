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

import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.data.input.kafka.KafkaTopicPartition;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.storage.StorageConnector;
import org.apache.druid.storage.local.LocalFileStorageConnectorProvider;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class ShareGroupRawBatchTest
{
  private final ShareGroupRawBatchCodec codec = new ShareGroupRawBatchCodec();

  @Test
  public void testRoundTripPreservesKafkaRecordMetadata() throws Exception
  {
    final ShareGroupRawBatch batch = batch();

    final ShareGroupRawBatch restored = codec.read(new ByteArrayInputStream(write(batch)));

    Assertions.assertEquals(batch, restored);
    Assertions.assertEquals(3, restored.getFirstOffset());
    Assertions.assertEquals(101, restored.getLastOffset());
    final List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> orderedRecords =
        restored.toOrderedRecords();
    Assertions.assertEquals(3, orderedRecords.size());
    Assertions.assertTrue(orderedRecords.get(1).getData().isEmpty());
    final ConsumerRecord<byte[], byte[]> nullValueRecord =
        ((KafkaShareGroupRecord) orderedRecords.get(1)).getConsumerRecord();
    Assertions.assertArrayEquals(new byte[]{9}, nullValueRecord.key());
    Assertions.assertNull(nullValueRecord.value());
    Assertions.assertEquals(Optional.of(4), nullValueRecord.leaderEpoch());
    Assertions.assertEquals(Optional.of((short) 2), nullValueRecord.deliveryCount());
    Assertions.assertEquals(2, nullValueRecord.headers().toArray().length);
    Assertions.assertEquals("duplicate", nullValueRecord.headers().toArray()[0].key());
    Assertions.assertEquals("duplicate", nullValueRecord.headers().toArray()[1].key());
  }

  @Test
  public void testDetectsTruncatedObject() throws Exception
  {
    final byte[] bytes = write(batch());

    Assertions.assertThrows(
        IOException.class,
        () -> codec.read(new ByteArrayInputStream(Arrays.copyOf(bytes, bytes.length - 5)))
    );
  }

  @Test
  public void testDetectsCorruptPayload() throws Exception
  {
    final byte[] bytes = write(batch());
    bytes[bytes.length / 2] ^= 1;

    Assertions.assertThrows(IOException.class, () -> codec.read(new ByteArrayInputStream(bytes)));
  }

  @Test
  public void testRejectsUnknownVersion() throws Exception
  {
    final byte[] bytes = write(batch());
    bytes[5] = 2;

    final IOException exception = Assertions.assertThrows(
        IOException.class,
        () -> codec.read(new ByteArrayInputStream(bytes))
    );
    Assertions.assertTrue(exception.getMessage().contains("version"));
  }

  @Test
  public void testRejectsUnsupportedCompression() throws Exception
  {
    final byte[] bytes = write(batch());
    bytes[6] = 2;

    final IOException exception = Assertions.assertThrows(
        IOException.class,
        () -> codec.read(new ByteArrayInputStream(bytes))
    );
    Assertions.assertTrue(exception.getMessage().contains("compression"));
  }

  @Test
  public void testRejectsWrongExpectedIdentity() throws Exception
  {
    final byte[] bytes = write(batch());

    final IOException exception = Assertions.assertThrows(
        IOException.class,
        () -> codec.read(new ByteArrayInputStream(bytes), "other-topic", 7)
    );
    Assertions.assertTrue(exception.getMessage().contains("identity mismatch"));
  }

  @Test
  public void testLargeValueRoundTrip() throws Exception
  {
    final byte[] value = new byte[8 << 20];
    Arrays.fill(value, (byte) 3);
    final ShareGroupRawBatch batch = new ShareGroupRawBatch(
        "topic",
        0,
        List.of(ShareGroupRawRecord.from(record(1, null, value, TimestampType.CREATE_TIME)))
    );

    final ShareGroupRawBatch restored = codec.read(new ByteArrayInputStream(write(batch)));

    Assertions.assertEquals(batch, restored);
  }

  @Test
  public void testLocalStoreWritesVerifiesAndUsesUniquePaths(@TempDir Path tempDir) throws Exception
  {
    final StorageConnector connector = new LocalFileStorageConnectorProvider(tempDir.toFile())
        .createStorageConnector(tempDir.toFile());
    final ShareGroupBatchStore store = new ShareGroupBatchStore(connector, "task/1");

    final ShareGroupBatchStore.StoredBatch first = store.store(batch());
    final ShareGroupBatchStore.StoredBatch second = store.store(batch());

    Assertions.assertNotEquals(first.getPath(), second.getPath());
    Assertions.assertTrue(connector.pathExists(first.getPath()));
    Assertions.assertEquals(64, first.getSha256().length());
    Assertions.assertTrue(first.getSize() > 0);
    Assertions.assertEquals(batch(), store.read(first));
  }

  @Test
  public void testLocalStoreDetectsCorruption(@TempDir Path tempDir) throws Exception
  {
    final StorageConnector connector = new LocalFileStorageConnectorProvider(tempDir.toFile())
        .createStorageConnector(tempDir.toFile());
    final ShareGroupBatchStore store = new ShareGroupBatchStore(connector, "task");
    final ShareGroupBatchStore.StoredBatch storedBatch = store.store(batch());
    Files.write(tempDir.resolve(storedBatch.getPath()), new byte[]{1, 2, 3});

    Assertions.assertThrows(IOException.class, () -> store.read(storedBatch));
  }

  private byte[] write(ShareGroupRawBatch batch) throws IOException
  {
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    codec.write(batch, output);
    return output.toByteArray();
  }

  private static ShareGroupRawBatch batch()
  {
    return new ShareGroupRawBatch(
        "topic-a",
        7,
        List.of(
            ShareGroupRawRecord.from(record(3, new byte[]{1}, new byte[]{2}, TimestampType.CREATE_TIME)),
            ShareGroupRawRecord.from(record(17, new byte[]{9}, null, TimestampType.LOG_APPEND_TIME)),
            ShareGroupRawRecord.from(record(101, null, new byte[0], TimestampType.NO_TIMESTAMP_TYPE))
        )
    );
  }

  private static ConsumerRecord<byte[], byte[]> record(
      long offset,
      byte[] key,
      byte[] value,
      TimestampType timestampType
  )
  {
    final RecordHeaders headers = new RecordHeaders();
    headers.add("duplicate", new byte[]{1});
    headers.add("duplicate", null);
    return new ConsumerRecord<>(
        "topic-a",
        7,
        offset,
        123_456L + offset,
        timestampType,
        key == null ? -1 : key.length,
        value == null ? -1 : value.length,
        key,
        value,
        headers,
        Optional.of(4),
        Optional.of((short) 2)
    );
  }
}
