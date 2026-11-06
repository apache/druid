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

import com.google.common.base.Supplier;
import org.apache.druid.data.input.Committer;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.data.input.kafka.KafkaTopicPartition;
import org.apache.druid.indexing.common.actions.SegmentTransactionalAppendFromShareInboxAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.indexing.overlord.ShareInboxCompletionRequest;
import org.apache.druid.indexing.overlord.ShareInboxManifest;
import org.apache.druid.indexing.seekablestream.StreamChunkReader;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.SegmentSchemaMapping;
import org.apache.druid.segment.realtime.appenderator.AppenderatorDriverAddResult;
import org.apache.druid.segment.realtime.appenderator.StreamAppenderatorDriver;
import org.apache.druid.segment.realtime.appenderator.TransactionalSegmentPublisher;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

final class AppenderatorShareInboxBatchHandler implements ShareInboxBatchHandler
{
  private final StreamAppenderatorDriver driver;
  private final StreamChunkReader<KafkaRecordEntity> chunkReader;
  private final TaskActionClient taskActionClient;
  private final String taskId;

  AppenderatorShareInboxBatchHandler(
      StreamAppenderatorDriver driver,
      StreamChunkReader<KafkaRecordEntity> chunkReader,
      TaskActionClient taskActionClient,
      String taskId
  )
  {
    this.driver = driver;
    this.chunkReader = chunkReader;
    this.taskActionClient = taskActionClient;
    this.taskId = taskId;
  }

  @Override
  public void process(
      List<ShareInboxManifest> manifests,
      List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> records,
      ShareInboxClaimLease claimLease
  ) throws Exception
  {
    final String completionId = UUID.randomUUID().toString();
    final String sequenceName = taskId + "-share-inbox-" + completionId;
    final Supplier<Committer> committerSupplier = () -> new Committer()
    {
      @Override
      public Object getMetadata()
      {
        return Map.of("shareInboxCompletionId", completionId);
      }

      @Override
      public void run()
      {
      }
    };

    boolean rowsAdded = false;
    for (OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity> record : records) {
      claimLease.assertValid();
      final List<InputRow> rows = chunkReader.parse(kafkaEntities(record), false);
      for (InputRow row : rows) {
        claimLease.assertValid();
        final AppenderatorDriverAddResult addResult = driver.add(
            row,
            sequenceName,
            committerSupplier,
            true,
            false
        );
        if (!addResult.isOk()) {
          throw new ISE("Could not allocate segment for share inbox row with timestamp[%s]", row.getTimestamp());
        }
        rowsAdded = true;
      }
    }
    if (rowsAdded) {
      claimLease.assertValid();
      driver.persist(committerSupplier.get());
    }

    final ShareInboxCompletionRequest completionRequest = completionRequest(manifests, completionId);
    final TransactionalSegmentPublisher publisher = new TransactionalSegmentPublisher()
    {
      @Override
      public SegmentPublishResult publishAnnotatedSegments(
          @Nullable Set<DataSegment> mustBeNullOrEmptyOverwriteSegments,
          Set<DataSegment> segmentsToPush,
          @Nullable Object commitMetadata,
          SegmentSchemaMapping segmentSchemaMapping
      ) throws IOException
      {
        claimLease.assertValid();
        return taskActionClient.submit(
            new SegmentTransactionalAppendFromShareInboxAction(
                segmentsToPush,
                completionRequest,
                segmentSchemaMapping
            )
        );
      }

      @Override
      public boolean supportsEmptyPublish()
      {
        return true;
      }
    };

    claimLease.assertValid();
    driver.publish(publisher, committerSupplier.get(), List.of(sequenceName)).get();
  }

  @SuppressWarnings("unchecked")
  private static List<? extends KafkaRecordEntity> kafkaEntities(
      OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity> record
  )
  {
    return (List<? extends KafkaRecordEntity>) (List<?>) record.getData();
  }

  private ShareInboxCompletionRequest completionRequest(
      List<ShareInboxManifest> manifests,
      String completionId
  )
  {
    if (manifests.isEmpty()) {
      throw new ISE("Cannot complete an empty share inbox manifest list");
    }
    final ShareInboxManifest first = manifests.get(0);
    final Map<String, Long> claims = new TreeMap<>();
    for (ShareInboxManifest manifest : manifests) {
      claims.put(manifest.getManifestId(), manifest.getClaimEpoch());
    }
    return new ShareInboxCompletionRequest(
        first.getDataSource(),
        first.getInboxId(),
        first.getSpecFingerprint(),
        first.getClaimOwner(),
        claims,
        completionId
    );
  }
}
