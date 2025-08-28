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
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Kafka indexing task runner that supports incremental segment publishing.
 */
public class KafkaIndexTaskRunner extends SeekableStreamIndexTaskRunner<KafkaTopicPartition, Long, KafkaRecordEntity>
{
  private static final EmittingLogger log = new EmittingLogger(KafkaIndexTaskRunner.class);
  private final KafkaIndexTask task;

  KafkaIndexTaskRunner(
      KafkaIndexTask task,
      @Nullable InputRowParser<ByteBuffer> parser,
      LockGranularity lockGranularityToUse
  )
  {
    super(
        task,
        parser,
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
      throw e;
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

