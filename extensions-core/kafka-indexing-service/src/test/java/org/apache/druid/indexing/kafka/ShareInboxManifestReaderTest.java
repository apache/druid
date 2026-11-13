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
import org.apache.druid.indexing.overlord.ShareInboxManifest;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.storage.StorageConnector;
import org.apache.druid.storage.local.LocalFileStorageConnectorProvider;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class ShareInboxManifestReaderTest
{
  @Test
  public void testReadsOnlySelectedOffsets(@TempDir Path tempDir) throws Exception
  {
    final ShareGroupBatchStore store = store(tempDir);
    final ShareGroupBatchStore.StoredBatch storedBatch = store.store(records(3, 17, 101));
    final ShareInboxManifestReader reader = reader(store);

    final List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> records = reader.read(
        manifest(storedBatch, "topic-id", List.of(101L, 3L))
    );

    Assertions.assertEquals(
        List.of(101L, 3L),
        records.stream().map(OrderedPartitionableRecord::getSequenceNumber).toList()
    );
  }

  @Test
  public void testRejectsManifestFromDifferentTopicGeneration(@TempDir Path tempDir) throws Exception
  {
    final ShareGroupBatchStore store = store(tempDir);
    final ShareGroupBatchStore.StoredBatch storedBatch = store.store(records(3));

    Assertions.assertThrows(
        IOException.class,
        () -> reader(store).read(manifest(storedBatch, "recreated-topic-id", List.of(3L)))
    );
  }

  @Test
  public void testRejectsSelectedOffsetMissingFromObject(@TempDir Path tempDir) throws Exception
  {
    final ShareGroupBatchStore store = store(tempDir);
    final ShareGroupBatchStore.StoredBatch storedBatch = store.store(records(3));

    Assertions.assertThrows(
        IOException.class,
        () -> reader(store).read(manifest(storedBatch, "topic-id", List.of(4L)))
    );
  }

  private static ShareGroupBatchStore store(Path tempDir)
  {
    final StorageConnector connector = new LocalFileStorageConnectorProvider(tempDir.toFile())
        .createStorageConnector(tempDir.toFile());
    return new ShareGroupBatchStore(connector, "task-1");
  }

  private static ShareInboxManifestReader reader(ShareGroupBatchStore store)
  {
    return new ShareInboxManifestReader(
        store,
        "datasource",
        "inbox",
        "group",
        new ShareGroupSourceIdentity("cluster-id", "topic-id", "topic"),
        "fingerprint"
    );
  }

  private static ShareInboxManifest manifest(
      ShareGroupBatchStore.StoredBatch storedBatch,
      String topicId,
      List<Long> selectedOffsets
  )
  {
    return new ShareInboxManifest(
        "manifest-1",
        "datasource",
        "inbox",
        "group",
        "cluster-id",
        topicId,
        "topic",
        2,
        "fingerprint",
        storedBatch.getPath(),
        storedBatch.getSha256(),
        storedBatch.getSize(),
        selectedOffsets,
        selectedOffsets.size(),
        "task-1",
        1,
        DateTimes.nowUtc().plusMinutes(1),
        1
    );
  }

  private static List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> records(
      long... offsets
  )
  {
    return java.util.Arrays.stream(offsets)
                           .mapToObj(ShareInboxManifestReaderTest::record)
                           .toList();
  }

  private static OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity> record(long offset)
  {
    return KafkaShareGroupRecord.from(new ConsumerRecord<>("topic", 2, offset, null, new byte[]{(byte) offset}));
  }
}
