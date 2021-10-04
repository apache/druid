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

package org.apache.druid.indexing.seekablestream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.indexing.seekablestream.common.OrderedSequenceNumber;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.java.util.common.granularity.AllGranularity;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.ArbitraryGranularitySpec;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

public class SeekableStreamIndexTaskRunnerTest
{

  private SeekableStreamIndexTaskRunnerImpl taskRunner;
  private SeekableStreamIndexTask<String, String, ByteEntity> indexTask;

  @Before
  public void setUp()
  {
    AuthorizerMapper authorizerMapper = new AuthorizerMapper(null)
    {
      @Override
      public Authorizer getAuthorizer(String name)
      {
        return super.getAuthorizer(name);
      }
    };

    DataSchema dataSchema = new DataSchema(
      "datasource",
      new TimestampSpec(null, null, null),
      new DimensionsSpec(Collections.emptyList()),
      new AggregatorFactory[]{},
      new ArbitraryGranularitySpec(new AllGranularity(), Collections.emptyList()),
      TransformSpec.NONE,
      null,
      null
    );

    SeekableStreamIndexTaskTuningConfig tuningConfig = mock(SeekableStreamIndexTaskTuningConfig.class);
    SeekableStreamIndexTaskIOConfig<String, String> ioConfig = mock(SeekableStreamIndexTaskIOConfig.class);

    indexTask = mock(SeekableStreamIndexTask.class);

    expect(indexTask.getIOConfig()).andReturn(ioConfig).atLeastOnce();
    expect(indexTask.getTuningConfig()).andReturn(tuningConfig).atLeastOnce();
    expect(indexTask.getDataSchema()).andReturn(dataSchema);

    replay(indexTask);
    taskRunner = new SeekableStreamIndexTaskRunnerImpl(indexTask, authorizerMapper);
  }

  @Test
  public void testGetStatus()
  {
    taskRunner.getStatus();
  }

  private class SeekableStreamIndexTaskRunnerImpl extends SeekableStreamIndexTaskRunner<String, String, ByteEntity>
  {

    private SeekableStreamIndexTaskRunnerImpl(
        SeekableStreamIndexTask<String, String, ByteEntity> task,
        AuthorizerMapper authorizerMapper
    )
    {
      super(task, null, authorizerMapper, LockGranularity.SEGMENT);
    }

    @Override
    protected boolean isEndOfShard(String seqNum)
    {
      return false;
    }

    @Nullable
    @Override
    protected TreeMap<Integer, Map<String, String>> getCheckPointsFromContext(
        TaskToolbox toolbox, String checkpointsString
    ) throws IOException
    {
      return null;
    }

    @Override
    protected String getNextStartOffset(String sequenceNumber)
    {
      return null;
    }

    @Override
    protected SeekableStreamEndSequenceNumbers<String, String> deserializePartitionsFromMetadata(
        ObjectMapper mapper, Object object
    )
    {
      return null;
    }

    @Nonnull
    @Override
    protected List<OrderedPartitionableRecord<String, String, ByteEntity>> getRecords(
        RecordSupplier<String, String, ByteEntity> recordSupplier, TaskToolbox toolbox
    ) throws Exception
    {
      return null;
    }

    @Override
    protected SeekableStreamDataSourceMetadata<String, String> createDataSourceMetadata(SeekableStreamSequenceNumbers<String, String> partitions)
    {
      return null;
    }

    @Override
    protected OrderedSequenceNumber<String> createSequenceNumber(String sequenceNumber)
    {
      return null;
    }

    @Override
    protected void possiblyResetDataSourceMetadata(
        TaskToolbox toolbox,
        RecordSupplier<String, String, ByteEntity> recordSupplier,
        Set<StreamPartition<String>> assignment
    )
    {

    }

    @Override
    protected boolean isEndOffsetExclusive()
    {
      return false;
    }

    @Override
    protected TypeReference<List<SequenceMetadata<String, String>>> getSequenceMetadataTypeReference()
    {
      return null;
    }
  }

  private class SeekableStreamIndexTaskIOConfigImpl extends SeekableStreamIndexTaskIOConfig<String, String>
  {
    public SeekableStreamIndexTaskIOConfigImpl(
        String baseSequenceName,
        SeekableStreamStartSequenceNumbers<String, String> startSequenceNumbers,
        SeekableStreamEndSequenceNumbers<String, String> endSequenceNumbers
    )
    {
      super(
          null,
          baseSequenceName,
          new SeekableStreamStartSequenceNumbers<>("abc", "def", Collections.emptyMap(), Collections.emptyMap(), ),
          endSequenceNumbers,
          false,
          DateTime.now().minusDays(2),
          DateTime.now(),
          null
      );
    }
  }

}