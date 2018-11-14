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
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
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
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class KinesisIndexTaskRunner extends SeekableStreamIndexTaskRunner<String, String>
{
  private static final EmittingLogger log = new EmittingLogger(KinesisIndexTaskRunner.class);
  private static final long POLL_TIMEOUT = 100;
  private volatile CopyOnWriteArrayList<SequenceMetadata> sequences;


  public KinesisIndexTaskRunner(
      KinesisIndexTask task,
      InputRowParser<ByteBuffer> parser,
      AuthorizerMapper authorizerMapper,
      Optional<ChatHandlerProvider> chatHandlerProvider,
      CircularBuffer<Throwable> savedParseExceptions,
      RowIngestionMetersFactory rowIngestionMetersFactory
  )
  {
    super(
        task,
        parser,
        authorizerMapper,
        chatHandlerProvider,
        savedParseExceptions,
        rowIngestionMetersFactory,
        false
    );


  }


  @Override
  protected String getNextSequenceNumber(
      RecordSupplier<String, String> recordSupplier, StreamPartition<String> partition, String sequenceNumber
  )
  {
    String sequence = recordSupplier.getPosition(partition);
    return sequence == null ? sequenceNumber : sequence;
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
  protected OrderedSequenceNumber<String> createSequencenNumber(String sequenceNumber)
  {
    return KinesisSequenceNumber.of(sequenceNumber);
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


}
