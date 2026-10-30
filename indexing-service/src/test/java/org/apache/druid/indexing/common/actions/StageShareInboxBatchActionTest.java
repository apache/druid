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

package org.apache.druid.indexing.common.actions;

import org.apache.druid.error.DruidException;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.ShareInboxBatch;
import org.apache.druid.indexing.overlord.ShareInboxStageResult;
import org.apache.druid.segment.TestHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StageShareInboxBatchActionTest
{
  @Test
  public void testSerde() throws Exception
  {
    final StageShareInboxBatchAction original = new StageShareInboxBatchAction(batch());

    final TaskAction<?> deserialized = TestHelper.JSON_MAPPER.readValue(
        TestHelper.JSON_MAPPER.writeValueAsBytes(original),
        TaskAction.class
    );

    Assertions.assertInstanceOf(StageShareInboxBatchAction.class, deserialized);
    final ShareInboxBatch deserializedBatch = ((StageShareInboxBatchAction) deserialized).getBatch();
    Assertions.assertEquals(original.getBatch().getManifestId(), deserializedBatch.getManifestId());
    Assertions.assertEquals(original.getBatch().getOffsets(), deserializedBatch.getOffsets());
    Assertions.assertEquals(original.getBatch().getReceiptPageSize(), deserializedBatch.getReceiptPageSize());
  }

  @Test
  public void testPerformStagesBatch()
  {
    final ShareInboxBatch batch = batch();
    final StageShareInboxBatchAction action = new StageShareInboxBatchAction(batch);
    final TaskActionToolbox toolbox = mock(TaskActionToolbox.class);
    final IndexerMetadataStorageCoordinator coordinator = mock(IndexerMetadataStorageCoordinator.class);
    final ShareInboxStageResult expected = new ShareInboxStageResult(
        ShareInboxStageResult.Status.STAGED,
        batch.getOffsets()
    );
    when(toolbox.getIndexerMetadataStorageCoordinator()).thenReturn(coordinator);
    when(coordinator.stageShareInboxBatch(batch)).thenReturn(expected);

    final ShareInboxStageResult actual = action.perform(NoopTask.forDatasource("datasource"), toolbox);

    Assertions.assertSame(expected, actual);
    verify(coordinator).stageShareInboxBatch(batch);
  }

  @Test
  public void testPerformRejectsDifferentDatasource()
  {
    final StageShareInboxBatchAction action = new StageShareInboxBatchAction(batch());
    final TaskActionToolbox toolbox = mock(TaskActionToolbox.class);
    final IndexerMetadataStorageCoordinator coordinator = mock(IndexerMetadataStorageCoordinator.class);
    when(toolbox.getIndexerMetadataStorageCoordinator()).thenReturn(coordinator);

    Assertions.assertThrows(
        DruidException.class,
        () -> action.perform(NoopTask.forDatasource("other"), toolbox)
    );
    verify(coordinator, never()).stageShareInboxBatch(action.getBatch());
  }

  private static ShareInboxBatch batch()
  {
    return new ShareInboxBatch(
        "datasource",
        "inbox",
        "group",
        "cluster",
        "topic-id",
        "topic",
        0,
        "fingerprint",
        "manifest",
        "path/manifest",
        "hash",
        100,
        List.of(1L, 3L),
        4_096
    );
  }
}
