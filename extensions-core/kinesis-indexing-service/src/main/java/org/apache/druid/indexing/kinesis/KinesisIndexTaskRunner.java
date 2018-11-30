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
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.seekablestream.SeekableStreamDataSourceMetadata;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTask;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner;
import org.apache.druid.indexing.seekablestream.SeekableStreamPartitions;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.indexing.seekablestream.common.OrderedSequenceNumber;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.java.util.emitter.EmittingLogger;
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

public class KinesisIndexTaskRunner extends SeekableStreamIndexTaskRunner<String, String>
{
  private static final EmittingLogger log = new EmittingLogger(KinesisIndexTaskRunner.class);
  private static final long POLL_TIMEOUT = 100;

  KinesisIndexTaskRunner(
      KinesisIndexTask task,
      KinesisRecordSupplier recordSupplier,
      InputRowParser<ByteBuffer> parser,
      AuthorizerMapper authorizerMapper,
      Optional<ChatHandlerProvider> chatHandlerProvider,
      CircularBuffer<Throwable> savedParseExceptions,
      RowIngestionMetersFactory rowIngestionMetersFactory
  )
  {
    super(
        task,
        recordSupplier,
        parser,
        authorizerMapper,
        chatHandlerProvider,
        savedParseExceptions,
        rowIngestionMetersFactory
    );
  }


  @Override
  protected String getSequenceNumberToStoreAfterRead(String sequenceNumber)
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
  protected SeekableStreamPartitions<String, String> createSeekableStreamPartitions(
      ObjectMapper mapper,
      Object obeject
  )
  {
    return mapper.convertValue(obeject, mapper.getTypeFactory().constructParametrizedType(
        SeekableStreamPartitions.class,
        SeekableStreamPartitions.class,
        String.class,
        String.class
    ));
  }

  @Override
  protected SeekableStreamDataSourceMetadata<String, String> createDataSourceMetadata(
      SeekableStreamPartitions<String, String> partitions
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
  protected Type getRunnerType()
  {
    return Type.KINESIS;
  }

  @Override
  protected SequenceMetadata createSequenceMetadata(
      int sequenceId,
      String sequenceName,
      Map<String, String> startOffsets,
      Map<String, String> endOffsets,
      boolean checkpointed,
      Set<String> exclusiveStartPartitions
  )
  {
    return new KinesisSequenceMetaData(
        sequenceId,
        sequenceName,
        startOffsets,
        endOffsets,
        checkpointed,
        exclusiveStartPartitions
    );
  }

  @Nullable
  @Override
  protected TreeMap<Integer, Map<String, String>> getCheckPointsFromContext(
      TaskToolbox toolbox,
      SeekableStreamIndexTask<String, String> task
  ) throws IOException
  {
    final String checkpointsString = task.getContextValue("checkpoints");
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

  private class KinesisSequenceMetaData extends SequenceMetadata
  {

    KinesisSequenceMetaData(
        int sequenceId,
        String sequenceName,
        Map<String, String> startOffsets,
        Map<String, String> endOffsets,
        boolean checkpointed,
        Set<String> exclusiveStartPartitions
    )
    {
      super(sequenceId, sequenceName, startOffsets, endOffsets, checkpointed, exclusiveStartPartitions);
    }

    @Override
    protected boolean canHandle(OrderedPartitionableRecord<String, String> record)
    {
      lock.lock();
      try {
        final OrderedSequenceNumber<String> partitionEndOffset = createSequenceNumber(endOffsets.get(record.getPartitionId()));
        final OrderedSequenceNumber<String> partitionStartOffset = createSequenceNumber(startOffsets.get(record.getPartitionId()));
        final OrderedSequenceNumber<String> recordOffset = createSequenceNumber(record.getSequenceNumber());
        return isOpen()
               && recordOffset != null
               && partitionEndOffset != null
               && partitionStartOffset != null
               && recordOffset.compareTo(partitionStartOffset)
                  >= (getExclusiveStartPartitions().contains(record.getPartitionId()) ? 1 : 0)
               && recordOffset.compareTo(partitionEndOffset) <= 0;
      }
      finally {
        lock.unlock();
      }
    }

  }


}
