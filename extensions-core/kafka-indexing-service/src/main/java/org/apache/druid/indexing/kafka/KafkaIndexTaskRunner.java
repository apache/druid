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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.constraints.NotNull;
import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.data.input.kafka.KafkaTopicPartition;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.seekablestream.SeekableStreamDataSourceMetadata;
import org.apache.druid.indexing.seekablestream.SeekableStreamEndSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner;
import org.apache.druid.indexing.seekablestream.SeekableStreamSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SequenceMetadata;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.indexing.seekablestream.common.OrderedSequenceNumber;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.indexing.seekablestream.supervisor.BoundedStreamConfig;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.TopicPartition;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * Kafka indexing task runner that supports incremental segment publishing.
 */
public class KafkaIndexTaskRunner extends SeekableStreamIndexTaskRunner<KafkaTopicPartition, Long, KafkaRecordEntity>
{
  private static final EmittingLogger log = new EmittingLogger(KafkaIndexTaskRunner.class);
  private final KafkaIndexTask task;

  KafkaIndexTaskRunner(
      KafkaIndexTask task,
      LockGranularity lockGranularityToUse
  )
  {
    super(task, lockGranularityToUse);
    this.task = task;
  }

  @Override
  protected Long getNextStartOffset(@NotNull Long sequenceNumber)
  {
    return sequenceNumber + 1;
  }

  @Nonnull
  @Override
  protected List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> getRecords(
      RecordSupplier<KafkaTopicPartition, Long, KafkaRecordEntity> recordSupplier,
      TaskToolbox toolbox
  ) throws Exception
  {
    try {
      return recordSupplier.poll(task.getIOConfig().getPollTimeout());
    }
    catch (OffsetOutOfRangeException e) {
      log.warn("OffsetOutOfRangeException with message [%s]", e.getMessage());

      if (task.getTuningConfig().isResetOffsetAutomatically()) {
        // Check if any partition has an offset that is below the earliest available offset.
        // If so, re-throw the exception so the supervisor can detect and handle the reset.
        final String stream = task.getIOConfig().getStartSequenceNumbers().getStream();
        final boolean isMultiTopic = task.getIOConfig().isMultiTopic();
        for (Map.Entry<TopicPartition, Long> entry : e.offsetOutOfRangePartitions().entrySet()) {
          final TopicPartition topicPartition = entry.getKey();
          final StreamPartition<KafkaTopicPartition> streamPartition = StreamPartition.of(
              stream,
              new KafkaTopicPartition(isMultiTopic, topicPartition.topic(), topicPartition.partition())
          );
          final Long earliestOffset = recordSupplier.getEarliestSequenceNumber(streamPartition);
          if (earliestOffset != null && earliestOffset > entry.getValue()) {
            // The requested offset is below the earliest available offset.
            // Re-throw to let the supervisor handle the reset.
            throw e;
          }
        }
      }

      // Offset is either in the future (not yet written) or a temporary issue.
      // Wait and retry instead of failing.
      log.warn("Retrying in %dms", task.getPollRetryMs());
      pollRetryLock.lockInterruptibly();
      try {
        long nanos = TimeUnit.MILLISECONDS.toNanos(task.getPollRetryMs());
        while (nanos > 0L && !pauseRequested && !stopRequested.get()) {
          nanos = isAwaitingRetry.awaitNanos(nanos);
        }
      }
      finally {
        pollRetryLock.unlock();
      }
      return Collections.emptyList();
    }
  }

  @Override
  protected SeekableStreamEndSequenceNumbers<KafkaTopicPartition, Long> deserializePartitionsFromMetadata(
      ObjectMapper mapper,
      Object object
  )
  {
    return mapper.convertValue(object, mapper.getTypeFactory().constructParametrizedType(
        SeekableStreamEndSequenceNumbers.class,
        SeekableStreamEndSequenceNumbers.class,
        KafkaTopicPartition.class,
        Long.class
    ));
  }

  @Override
  protected SeekableStreamDataSourceMetadata<KafkaTopicPartition, Long> createDataSourceMetadata(
      SeekableStreamSequenceNumbers<KafkaTopicPartition, Long> partitions
  )
  {
    // Include bounded config if this is a bounded task
    BoundedStreamConfig boundedConfig = task.getIOConfig().getBoundedStreamConfig();
    return new KafkaDataSourceMetadata(partitions, boundedConfig);
  }

  @Override
  protected OrderedSequenceNumber<Long> createSequenceNumber(Long sequenceNumber)
  {
    return KafkaSequenceNumber.of(sequenceNumber);
  }

  @Override
  protected void possiblyResetDataSourceMetadata(
      TaskToolbox toolbox,
      RecordSupplier<KafkaTopicPartition, Long, KafkaRecordEntity> recordSupplier,
      Set<StreamPartition<KafkaTopicPartition>> assignment
  )
  {
    // do nothing
  }

  @Override
  protected boolean isEndOffsetExclusive()
  {
    return true;
  }

  @Override
  protected boolean isEndOfShard(Long seqNum)
  {
    return false;
  }

  @Override
  public TypeReference<List<SequenceMetadata<KafkaTopicPartition, Long>>> getSequenceMetadataTypeReference()
  {
    return new TypeReference<>() {};
  }

  @Nullable
  @Override
  protected TreeMap<Integer, Map<KafkaTopicPartition, Long>> getCheckPointsFromContext(
      TaskToolbox toolbox,
      String checkpointsString
  ) throws IOException
  {
    if (checkpointsString != null) {
      log.debug("Got checkpoints from task context[%s].", checkpointsString);
      return toolbox.getJsonMapper().readValue(
          checkpointsString,
          new TypeReference<>() {}
      );
    } else {
      return null;
    }
  }
}

