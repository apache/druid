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

package org.apache.druid.indexing.pulsar;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.pulsar.PulsarRecordEntity;
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
import org.apache.druid.server.security.AuthorizerMapper;

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
 * Pulsar indexing task runner supporting incremental segments publishing
 */
public class PulsarIndexTaskRunner extends SeekableStreamIndexTaskRunner<Integer, String, PulsarRecordEntity>
{
  private static final EmittingLogger log = new EmittingLogger(PulsarIndexTaskRunner.class);
  private final PulsarIndexTask task;

  PulsarIndexTaskRunner(
      PulsarIndexTask task,
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
  protected String getNextStartOffset(@NotNull String sequenceNumber)
  {
    return sequenceNumber;
  }

  @Nonnull
  @Override
  protected List<OrderedPartitionableRecord<Integer, String, PulsarRecordEntity>> getRecords(
      RecordSupplier<Integer, String, PulsarRecordEntity> recordSupplier,
      TaskToolbox toolbox
  )
  {
    List<OrderedPartitionableRecord<Integer, String, PulsarRecordEntity>> records;
    records = recordSupplier.poll(task.getIOConfig().getPollTimeout());
    return records;
  }

  @Override
  protected SeekableStreamEndSequenceNumbers<Integer, String> deserializePartitionsFromMetadata(
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
  protected SeekableStreamDataSourceMetadata<Integer, String> createDataSourceMetadata(
      SeekableStreamSequenceNumbers<Integer, String> partitions
  )
  {
    return new PulsarDataSourceMetadata(partitions);
  }

  @Override
  protected OrderedSequenceNumber<String> createSequenceNumber(String sequenceNumber)
  {
    return PulsarSequenceNumber.of(sequenceNumber);
  }

  @Override
  protected void possiblyResetDataSourceMetadata(
      TaskToolbox toolbox,
      RecordSupplier<Integer, String, PulsarRecordEntity> recordSupplier,
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
  protected boolean isEndOfShard(String seqNum)
  {
    return false;
  }

  @Override
  public TypeReference<List<SequenceMetadata<Integer, String>>> getSequenceMetadataTypeReference()
  {
    return new TypeReference<List<SequenceMetadata<Integer, String>>>()
    {
    };
  }

  @Nullable
  @Override
  protected TreeMap<Integer, Map<Integer, String>> getCheckPointsFromContext(
      TaskToolbox toolbox,
      String checkpointsString
  ) throws IOException
  {
    if (checkpointsString != null) {
      log.debug("Got checkpoints from task context[%s].", checkpointsString);
      return toolbox.getJsonMapper().readValue(
          checkpointsString,
          new TypeReference<TreeMap<Integer, Map<Integer, String>>>()
          {
          }
      );
    } else {
      return null;
    }
  }
}

