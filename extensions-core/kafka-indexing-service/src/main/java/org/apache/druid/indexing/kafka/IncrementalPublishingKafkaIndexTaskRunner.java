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
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.kafka.KafkaRecordEntity;
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
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.utils.CollectionUtils;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.TopicPartition;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * Kafka indexing task runner supporting incremental segments publishing
 */
public class IncrementalPublishingKafkaIndexTaskRunner extends SeekableStreamIndexTaskRunner<Integer, Long, KafkaRecordEntity>
{
  private static final EmittingLogger log = new EmittingLogger(IncrementalPublishingKafkaIndexTaskRunner.class);
  private final KafkaIndexTask task;

  IncrementalPublishingKafkaIndexTaskRunner(
      KafkaIndexTask task,
      @Nullable InputRowParser<ByteBuffer> parser,
      AuthorizerMapper authorizerMapper,
      LockGranularity lockGranularityToUse
  )
  {
    super(
        task,
        parser,
        authorizerMapper,
        lockGranularityToUse
    );
    this.task = task;
  }

  @Override
  protected Long getNextStartOffset(@NotNull Long sequenceNumber)
  {
    return sequenceNumber + 1;
  }

  @Nonnull
  @Override
  protected List<OrderedPartitionableRecord<Integer, Long, KafkaRecordEntity>> getRecords(
      RecordSupplier<Integer, Long, KafkaRecordEntity> recordSupplier,
      TaskToolbox toolbox
  ) throws Exception
  {
    try {
      return recordSupplier.poll(task.getIOConfig().getPollTimeout());
    }
    catch (OffsetOutOfRangeException e) {
      //
      // Handles OffsetOutOfRangeException, which is thrown if the seeked-to
      // offset is not present in the topic-partition. This can happen if we're asking a task to read from data
      // that has not been written yet (which is totally legitimate). So let's wait for it to show up
      //
      log.warn("OffsetOutOfRangeException with message [%s]", e.getMessage());
      possiblyResetOffsetsOrWait(e.offsetOutOfRangePartitions(), recordSupplier, toolbox);
      return Collections.emptyList();
    }
  }

  @Override
  protected SeekableStreamEndSequenceNumbers<Integer, Long> deserializePartitionsFromMetadata(
      ObjectMapper mapper,
      Object object
  )
  {
    return mapper.convertValue(object, mapper.getTypeFactory().constructParametrizedType(
        SeekableStreamEndSequenceNumbers.class,
        SeekableStreamEndSequenceNumbers.class,
        Integer.class,
        Long.class
    ));
  }

  private void possiblyResetOffsetsOrWait(
      Map<TopicPartition, Long> outOfRangePartitions,
      RecordSupplier<Integer, Long, KafkaRecordEntity> recordSupplier,
      TaskToolbox taskToolbox
  ) throws InterruptedException, IOException
  {
    final Map<TopicPartition, Long> newOffsetInMetadata = new HashMap<>();

    if (task.getTuningConfig().isResetOffsetAutomatically()) {
      for (Map.Entry<TopicPartition, Long> outOfRangePartition : outOfRangePartitions.entrySet()) {
        final TopicPartition topicPartition = outOfRangePartition.getKey();
        final long outOfRangeOffset = outOfRangePartition.getValue();

        StreamPartition<Integer> streamPartition = StreamPartition.of(
            topicPartition.topic(),
            topicPartition.partition()
        );

        final Long earliestAvailableOffset = recordSupplier.getEarliestSequenceNumber(streamPartition);
        if (earliestAvailableOffset == null) {
          throw new ISE("got null earliest sequence number for partition[%s] when fetching from kafka!",
                        topicPartition.partition());
        }

        if (outOfRangeOffset < earliestAvailableOffset) {
          //
          // In this case, it's probably because partition expires before the Druid could read from next offset
          // so the messages in [outOfRangeOffset, earliestAvailableOffset) is lost.
          // These lost messages could not be restored even a manual reset is performed
          // So, it's reasonable to reset the offset the earliest available position
          //
          recordSupplier.seek(streamPartition, earliestAvailableOffset);
          newOffsetInMetadata.put(topicPartition, outOfRangeOffset);
        } else {
          //
          // There are two cases in theory here
          // 1. outOfRangeOffset is in the range of [earliestAvailableOffset, latestAvailableOffset]
          // 2. outOfRangeOffset is larger than latestAvailableOffset
          //
          // for scenario 1, we do nothing but just wait for a period time to retry
          //                 since current offset is valid but maybe due to some temporary problem
          //
          // for scenario 2, how could this happen?
          // Well, if the task first consumes from a topic on cluster A,
          // and then supervisor spec is changed to consume from a same topic, where there are messages in this topic, on cluster B,
          // this can lead to this case.
          // For such case,
          // offsets stored in meta should be cleared when submitting the supervisor spec,
          // so the problem won't be left to manual reset or auto reset. Thus, we don't need to handle this complicated case here
          //
        }
      }
    }

    if (!newOffsetInMetadata.isEmpty()) {
      sendResetRequestAndWait(CollectionUtils.mapKeys(newOffsetInMetadata, streamPartition -> StreamPartition.of(
          streamPartition.topic(),
          streamPartition.partition()
      )), taskToolbox);
    } else {
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
    }
  }

  @Override
  protected SeekableStreamDataSourceMetadata<Integer, Long> createDataSourceMetadata(
      SeekableStreamSequenceNumbers<Integer, Long> partitions
  )
  {
    return new KafkaDataSourceMetadata(partitions);
  }

  @Override
  protected OrderedSequenceNumber<Long> createSequenceNumber(Long sequenceNumber)
  {
    return KafkaSequenceNumber.of(sequenceNumber);
  }

  @Override
  protected void possiblyResetDataSourceMetadata(
      TaskToolbox toolbox,
      RecordSupplier<Integer, Long, KafkaRecordEntity> recordSupplier,
      Set<StreamPartition<Integer>> assignment
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
  public TypeReference<List<SequenceMetadata<Integer, Long>>> getSequenceMetadataTypeReference()
  {
    return new TypeReference<List<SequenceMetadata<Integer, Long>>>()
    {
    };
  }

  @Nullable
  @Override
  protected TreeMap<Integer, Map<Integer, Long>> getCheckPointsFromContext(
      TaskToolbox toolbox,
      String checkpointsString
  ) throws IOException
  {
    if (checkpointsString != null) {
      log.debug("Got checkpoints from task context[%s].", checkpointsString);
      return toolbox.getJsonMapper().readValue(
          checkpointsString,
          new TypeReference<TreeMap<Integer, Map<Integer, Long>>>()
          {
          }
      );
    } else {
      return null;
    }
  }
}

