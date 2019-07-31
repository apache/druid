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

package org.apache.druid.indexing.kinesis;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
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
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.utils.CircularBuffer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class KinesisIndexTaskRunner extends SeekableStreamIndexTaskRunner<String, String>
{
  private static final EmittingLogger log = new EmittingLogger(KinesisIndexTaskRunner.class);
  private static final long POLL_TIMEOUT = 100;

  private final KinesisIndexTask task;

  KinesisIndexTaskRunner(
      KinesisIndexTask task,
      InputRowParser<ByteBuffer> parser,
      AuthorizerMapper authorizerMapper,
      Optional<ChatHandlerProvider> chatHandlerProvider,
      CircularBuffer<Throwable> savedParseExceptions,
      RowIngestionMetersFactory rowIngestionMetersFactory,
      AppenderatorsManager appenderatorsManager,
      LockGranularity lockGranularityToUse
  )
  {
    super(
        task,
        parser,
        authorizerMapper,
        chatHandlerProvider,
        savedParseExceptions,
        rowIngestionMetersFactory,
        appenderatorsManager,
        lockGranularityToUse
    );
    this.task = task;
  }


  @Override
  protected String getNextStartOffset(String sequenceNumber)
  {
    return sequenceNumber;
  }

  @Nonnull
  @Override
  protected List<OrderedPartitionableRecord<String, String>> getRecords(
      RecordSupplier<String, String> recordSupplier, TaskToolbox toolbox
  )
  {
    return recordSupplier.poll(POLL_TIMEOUT);
  }

  @Override
  protected SeekableStreamEndSequenceNumbers<String, String> deserializePartitionsFromMetadata(
      ObjectMapper mapper,
      Object object
  )
  {
    return mapper.convertValue(object, mapper.getTypeFactory().constructParametrizedType(
        SeekableStreamEndSequenceNumbers.class,
        SeekableStreamEndSequenceNumbers.class,
        String.class,
        String.class
    ));
  }

  @Override
  protected SeekableStreamDataSourceMetadata<String, String> createDataSourceMetadata(
      SeekableStreamSequenceNumbers<String, String> partitions
  )
  {
    return new KinesisDataSourceMetadata(partitions);
  }

  @Override
  protected OrderedSequenceNumber<String> createSequenceNumber(String sequenceNumber)
  {
    return KinesisSequenceNumber.of(sequenceNumber);
  }

  @Override
  protected void possiblyResetDataSourceMetadata(
      TaskToolbox toolbox,
      RecordSupplier<String, String> recordSupplier,
      Set<StreamPartition<String>> assignment
  )
  {
    if (!task.getTuningConfig().isSkipSequenceNumberAvailabilityCheck()) {
      final ConcurrentMap<String, String> currOffsets = getCurrentOffsets();
      for (final StreamPartition<String> streamPartition : assignment) {
        String sequence = currOffsets.get(streamPartition.getPartitionId());
        String earliestSequenceNumber = recordSupplier.getEarliestSequenceNumber(streamPartition);
        if (earliestSequenceNumber == null
            || createSequenceNumber(earliestSequenceNumber).compareTo(createSequenceNumber(sequence)) > 0) {
          if (task.getTuningConfig().isResetOffsetAutomatically()) {
            log.info("Attempting to reset sequences automatically for all partitions");
            try {
              sendResetRequestAndWait(
                  assignment.stream()
                            .collect(Collectors.toMap(x -> x, x -> currOffsets.get(x.getPartitionId()))),
                  toolbox
              );
            }
            catch (IOException e) {
              throw new ISE(e, "Exception while attempting to automatically reset sequences");
            }
          } else {
            throw new ISE(
                "Starting sequenceNumber [%s] is no longer available for partition [%s] (earliest: [%s]) and resetOffsetAutomatically is not enabled",
                sequence,
                streamPartition.getPartitionId(),
                earliestSequenceNumber
            );
          }
        }
      }
    }
  }

  @Override
  protected boolean isEndOffsetExclusive()
  {
    return false;
  }

  @Override
  protected boolean isEndOfShard(String seqNum)
  {
    return KinesisSequenceNumber.END_OF_SHARD_MARKER.equals(seqNum);
  }

  @Override
  public TypeReference<List<SequenceMetadata<String, String>>> getSequenceMetadataTypeReference()
  {
    return new TypeReference<List<SequenceMetadata<String, String>>>()
    {
    };
  }

  @Nullable
  @Override
  protected TreeMap<Integer, Map<String, String>> getCheckPointsFromContext(
      TaskToolbox toolbox,
      String checkpointsString
  ) throws IOException
  {
    if (checkpointsString != null) {
      log.info("Checkpoints [%s]", checkpointsString);
      return toolbox.getObjectMapper().readValue(
          checkpointsString,
          new TypeReference<TreeMap<Integer, Map<String, String>>>()
          {
          }
      );
    } else {
      return null;
    }
  }

}
