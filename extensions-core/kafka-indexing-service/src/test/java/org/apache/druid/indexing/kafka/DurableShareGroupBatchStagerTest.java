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
import org.apache.druid.indexing.common.actions.StageShareInboxBatchAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.overlord.ShareInboxBatch;
import org.apache.druid.indexing.overlord.ShareInboxStageResult;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.storage.StorageConnector;
import org.apache.druid.storage.local.LocalFileStorageConnectorProvider;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class DurableShareGroupBatchStagerTest
{
  @Test
  public void testStageRetainsReferencedObjectAndSendsStableIdentity(@TempDir Path tempDir) throws Exception
  {
    final StorageConnector connector = connector(tempDir);
    final TaskActionClient actionClient = Mockito.mock(TaskActionClient.class);
    Mockito.when(actionClient.submit(Mockito.any(StageShareInboxBatchAction.class)))
           .thenReturn(new ShareInboxStageResult(ShareInboxStageResult.Status.STAGED, List.of(3L, 7L)));
    final DurableShareGroupBatchStager stager = stager(connector, actionClient);

    stager.stage(records(3, 7));

    final ArgumentCaptor<StageShareInboxBatchAction> actionCaptor =
        ArgumentCaptor.forClass(StageShareInboxBatchAction.class);
    Mockito.verify(actionClient).submit(actionCaptor.capture());
    final ShareInboxBatch batch = actionCaptor.getValue().getBatch();
    Assertions.assertEquals("datasource", batch.getDataSource());
    Assertions.assertEquals("inbox", batch.getInboxId());
    Assertions.assertEquals("group", batch.getGroupId());
    Assertions.assertEquals("cluster-id", batch.getClusterId());
    Assertions.assertEquals("topic-id", batch.getTopicId());
    Assertions.assertEquals("topic", batch.getTopicName());
    Assertions.assertEquals(2, batch.getPartitionId());
    Assertions.assertEquals(List.of(3L, 7L), batch.getOffsets());
    Assertions.assertEquals(64, batch.getObjectHash().length());
    Assertions.assertTrue(connector.pathExists(batch.getObjectPath()));
  }

  @Test
  public void testAlreadyStagedDeletesUnreferencedObject(@TempDir Path tempDir) throws Exception
  {
    final StorageConnector connector = connector(tempDir);
    final TaskActionClient actionClient = Mockito.mock(TaskActionClient.class);
    Mockito.when(actionClient.submit(Mockito.any(StageShareInboxBatchAction.class)))
           .thenReturn(new ShareInboxStageResult(ShareInboxStageResult.Status.ALREADY_STAGED, List.of()));
    final DurableShareGroupBatchStager stager = stager(connector, actionClient);

    stager.stage(records(3));

    final ArgumentCaptor<StageShareInboxBatchAction> actionCaptor =
        ArgumentCaptor.forClass(StageShareInboxBatchAction.class);
    Mockito.verify(actionClient).submit(actionCaptor.capture());
    Assertions.assertFalse(connector.pathExists(actionCaptor.getValue().getBatch().getObjectPath()));
  }

  @Test
  public void testLostStageResponseLeavesObjectForSafeRecovery(@TempDir Path tempDir) throws Exception
  {
    final StorageConnector connector = connector(tempDir);
    final TaskActionClient actionClient = Mockito.mock(TaskActionClient.class);
    Mockito.when(actionClient.submit(Mockito.any(StageShareInboxBatchAction.class)))
           .thenThrow(new IOException("response lost"));
    final DurableShareGroupBatchStager stager = stager(connector, actionClient);

    Assertions.assertThrows(IOException.class, () -> stager.stage(records(3)));

    final ArgumentCaptor<StageShareInboxBatchAction> actionCaptor =
        ArgumentCaptor.forClass(StageShareInboxBatchAction.class);
    Mockito.verify(actionClient).submit(actionCaptor.capture());
    Assertions.assertTrue(connector.pathExists(actionCaptor.getValue().getBatch().getObjectPath()));
  }

  @Test
  public void testRejectsInvalidMetadataResponse(@TempDir Path tempDir) throws Exception
  {
    final StorageConnector connector = connector(tempDir);
    final TaskActionClient actionClient = Mockito.mock(TaskActionClient.class);
    Mockito.when(actionClient.submit(Mockito.any(StageShareInboxBatchAction.class)))
           .thenReturn(new ShareInboxStageResult(ShareInboxStageResult.Status.STAGED, List.of(99L)));
    final DurableShareGroupBatchStager stager = stager(connector, actionClient);

    Assertions.assertThrows(RuntimeException.class, () -> stager.stage(records(3)));
  }

  private static DurableShareGroupBatchStager stager(
      StorageConnector connector,
      TaskActionClient actionClient
  )
  {
    return new DurableShareGroupBatchStager(
        new ShareGroupBatchStore(connector, "task"),
        actionClient,
        "datasource",
        "inbox",
        "group",
        new ShareGroupSourceIdentity("cluster-id", "topic-id", "topic"),
        "fingerprint",
        4_096
    );
  }

  private static StorageConnector connector(Path tempDir)
  {
    return new LocalFileStorageConnectorProvider(tempDir.toFile()).createStorageConnector(tempDir.toFile());
  }

  private static List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> records(
      long... offsets
  )
  {
    return java.util.Arrays.stream(offsets)
                           .mapToObj(DurableShareGroupBatchStagerTest::record)
                           .toList();
  }

  private static OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity> record(long offset)
  {
    final ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
        "topic",
        2,
        offset,
        new byte[]{1},
        new byte[]{2}
    );
    return KafkaShareGroupRecord.from(record);
  }
}
