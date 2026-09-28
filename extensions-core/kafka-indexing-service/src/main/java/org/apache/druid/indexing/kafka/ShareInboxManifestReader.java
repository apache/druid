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
import org.apache.druid.indexing.overlord.ShareInboxManifest;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class ShareInboxManifestReader implements ShareInboxRecordSource
{
  private final ShareGroupBatchStore batchStore;
  private final String dataSource;
  private final String inboxId;
  private final String groupId;
  private final ShareGroupSourceIdentity sourceIdentity;
  private final String specFingerprint;

  ShareInboxManifestReader(
      ShareGroupBatchStore batchStore,
      String dataSource,
      String inboxId,
      String groupId,
      ShareGroupSourceIdentity sourceIdentity,
      String specFingerprint
  )
  {
    this.batchStore = Preconditions.checkNotNull(batchStore, "batchStore");
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.inboxId = Preconditions.checkNotNull(inboxId, "inboxId");
    this.groupId = Preconditions.checkNotNull(groupId, "groupId");
    this.sourceIdentity = Preconditions.checkNotNull(sourceIdentity, "sourceIdentity");
    this.specFingerprint = Preconditions.checkNotNull(specFingerprint, "specFingerprint");
  }

  @Override
  public List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> read(
      ShareInboxManifest manifest
  ) throws IOException
  {
    validateManifest(manifest);
    final ShareGroupRawBatch batch = batchStore.read(
        manifest.getObjectPath(),
        manifest.getObjectHash(),
        manifest.getTopicName(),
        manifest.getPartitionId()
    );
    final Map<Long, OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> recordsByOffset =
        new HashMap<>();
    for (OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity> record : batch.toOrderedRecords()) {
      if (recordsByOffset.put(record.getSequenceNumber(), record) != null) {
        throw new IOException("Share inbox object contains duplicate offset " + record.getSequenceNumber());
      }
    }

    final List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> selectedRecords =
        new ArrayList<>(manifest.getRecordCount());
    final Set<Long> selectedOffsets = new HashSet<>();
    for (Long offset : manifest.getSelectedOffsets()) {
      if (!selectedOffsets.add(offset)) {
        throw new IOException("Share inbox manifest contains duplicate selected offset " + offset);
      }
      final OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity> record =
          recordsByOffset.get(offset);
      if (record == null) {
        throw new IOException("Share inbox object does not contain selected offset " + offset);
      }
      selectedRecords.add(record);
    }
    return selectedRecords;
  }

  private void validateManifest(ShareInboxManifest manifest) throws IOException
  {
    if (!dataSource.equals(manifest.getDataSource())
        || !inboxId.equals(manifest.getInboxId())
        || !groupId.equals(manifest.getGroupId())
        || !sourceIdentity.getClusterId().equals(manifest.getClusterId())
        || !sourceIdentity.getTopicId().equals(manifest.getTopicId())
        || !sourceIdentity.getTopicName().equals(manifest.getTopicName())
        || !specFingerprint.equals(manifest.getSpecFingerprint())) {
      throw new IOException("Share inbox manifest identity does not match the active ingestion generation");
    }
  }
}
