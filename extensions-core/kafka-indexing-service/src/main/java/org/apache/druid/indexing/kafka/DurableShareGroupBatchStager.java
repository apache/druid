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

import com.google.common.base.Preconditions;
import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.data.input.kafka.KafkaTopicPartition;
import org.apache.druid.indexing.common.actions.StageShareInboxBatchAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.overlord.ShareInboxBatch;
import org.apache.druid.indexing.overlord.ShareInboxStageResult;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

final class DurableShareGroupBatchStager implements ShareGroupBatchStager
{
  private static final Logger log = new Logger(DurableShareGroupBatchStager.class);

  private final ShareGroupBatchStore batchStore;
  private final TaskActionClient taskActionClient;
  private final String dataSource;
  private final String inboxId;
  private final String groupId;
  private final ShareGroupSourceIdentity sourceIdentity;
  private final String specFingerprint;
  private final int receiptPageSize;

  DurableShareGroupBatchStager(
      ShareGroupBatchStore batchStore,
      TaskActionClient taskActionClient,
      String dataSource,
      String inboxId,
      String groupId,
      ShareGroupSourceIdentity sourceIdentity,
      String specFingerprint,
      int receiptPageSize
  )
  {
    this.batchStore = Preconditions.checkNotNull(batchStore, "batchStore");
    this.taskActionClient = Preconditions.checkNotNull(taskActionClient, "taskActionClient");
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.inboxId = Preconditions.checkNotNull(inboxId, "inboxId");
    this.groupId = Preconditions.checkNotNull(groupId, "groupId");
    this.sourceIdentity = Preconditions.checkNotNull(sourceIdentity, "sourceIdentity");
    this.specFingerprint = Preconditions.checkNotNull(specFingerprint, "specFingerprint");
    Preconditions.checkArgument(receiptPageSize > 0, "receiptPageSize must be positive");
    this.receiptPageSize = receiptPageSize;
  }

  @Override
  public void stage(
      List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> records
  ) throws IOException
  {
    final ShareGroupBatchStore.StoredBatch storedBatch = batchStore.store(records);
    final ShareGroupRawBatch rawBatch = storedBatch.getBatch();
    final List<Long> offsets = rawBatch.getRecords().stream()
                                           .map(ShareGroupRawRecord::getOffset)
                                           .toList();
    final ShareInboxBatch inboxBatch = new ShareInboxBatch(
        dataSource,
        inboxId,
        groupId,
        sourceIdentity.getClusterId(),
        sourceIdentity.getTopicId(),
        sourceIdentity.getTopicName(),
        rawBatch.getPartition(),
        specFingerprint,
        UUID.randomUUID().toString(),
        storedBatch.getPath(),
        storedBatch.getSha256(),
        storedBatch.getSize(),
        offsets,
        receiptPageSize
    );
    final ShareInboxStageResult result = taskActionClient.submit(new StageShareInboxBatchAction(inboxBatch));
    validateResult(result, offsets);
    if (result.getStatus() == ShareInboxStageResult.Status.ALREADY_STAGED) {
      try {
        batchStore.delete(storedBatch);
      }
      catch (IOException e) {
        log.warn(e, "Unable to delete unreferenced share inbox object[%s].", storedBatch.getPath());
      }
    }
  }

  private static void validateResult(ShareInboxStageResult result, List<Long> requestedOffsets)
  {
    if (result == null) {
      throw new ISE("Share inbox stage action returned no result");
    }
    final Set<Long> requested = new HashSet<>(requestedOffsets);
    if (!requested.containsAll(result.getAdmittedOffsets())) {
      throw new ISE("Share inbox stage action admitted offsets outside the requested batch");
    }
    if (result.getStatus() == ShareInboxStageResult.Status.STAGED && result.getAdmittedOffsets().isEmpty()) {
      throw new ISE("Share inbox stage action returned STAGED without admitted offsets");
    }
    if (result.getStatus() == ShareInboxStageResult.Status.ALREADY_STAGED
        && !result.getAdmittedOffsets().isEmpty()) {
      throw new ISE("Share inbox stage action returned ALREADY_STAGED with admitted offsets");
    }
  }
}
