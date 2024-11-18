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
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.indexing.seekablestream.common.OrderedSequenceNumber;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.server.security.AuthorizerMapper;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

@RunWith(MockitoJUnitRunner.class)
public class SeekableStreamIndexTaskRunnerTest
{
  @Mock
  private InputRow row;

  @Mock
  private SeekableStreamIndexTask task;

  @Test
  public void testWithinMinMaxTime()
  {
    DimensionsSpec dimensionsSpec = new DimensionsSpec(
        Arrays.asList(
            new StringDimensionSchema("d1"),
            new StringDimensionSchema("d2")
        )
    );
    DataSchema schema =
        DataSchema.builder()
                  .withDataSource("datasource")
                  .withTimestamp(new TimestampSpec(null, null, null))
                  .withDimensions(dimensionsSpec)
                  .withGranularity(
                      new UniformGranularitySpec(Granularities.MINUTE, Granularities.NONE, null)
                  )
                  .build();

    SeekableStreamIndexTaskTuningConfig tuningConfig = Mockito.mock(SeekableStreamIndexTaskTuningConfig.class);
    SeekableStreamIndexTaskIOConfig<String, String> ioConfig = Mockito.mock(SeekableStreamIndexTaskIOConfig.class);
    SeekableStreamStartSequenceNumbers<String, String> sequenceNumbers = Mockito.mock(SeekableStreamStartSequenceNumbers.class);
    SeekableStreamEndSequenceNumbers<String, String> endSequenceNumbers = Mockito.mock(SeekableStreamEndSequenceNumbers.class);

    DateTime now = DateTimes.nowUtc();

    Mockito.when(ioConfig.getRefreshRejectionPeriodsInMinutes()).thenReturn(120L);
    Mockito.when(ioConfig.getMaximumMessageTime()).thenReturn(Optional.of(DateTimes.nowUtc().plusHours(2)));
    Mockito.when(ioConfig.getMinimumMessageTime()).thenReturn(Optional.of(DateTimes.nowUtc().minusHours(2)));
    Mockito.when(ioConfig.getInputFormat()).thenReturn(new JsonInputFormat(null, null, null, null, null));
    Mockito.when(ioConfig.getStartSequenceNumbers()).thenReturn(sequenceNumbers);
    Mockito.when(ioConfig.getEndSequenceNumbers()).thenReturn(endSequenceNumbers);

    Mockito.when(endSequenceNumbers.getPartitionSequenceNumberMap()).thenReturn(ImmutableMap.of());
    Mockito.when(sequenceNumbers.getStream()).thenReturn("test");

    Mockito.when(task.getDataSchema()).thenReturn(schema);
    Mockito.when(task.getIOConfig()).thenReturn(ioConfig);
    Mockito.when(task.getTuningConfig()).thenReturn(tuningConfig);

    TestasbleSeekableStreamIndexTaskRunner runner = new TestasbleSeekableStreamIndexTaskRunner(task, null, null, LockGranularity.TIME_CHUNK);

    Mockito.when(row.getTimestamp()).thenReturn(now);
    Assert.assertTrue(runner.withinMinMaxRecordTime(row));

    Mockito.when(row.getTimestamp()).thenReturn(now.minusHours(2).minusMinutes(1));
    Assert.assertFalse(runner.withinMinMaxRecordTime(row));

    Mockito.when(row.getTimestamp()).thenReturn(now.plusHours(2).plusMinutes(1));
    Assert.assertFalse(runner.withinMinMaxRecordTime(row));
  }

  @Test
  public void testWithinMinMaxTimeNotPopulated()
  {
    DimensionsSpec dimensionsSpec = new DimensionsSpec(
        Arrays.asList(
            new StringDimensionSchema("d1"),
            new StringDimensionSchema("d2")
        )
    );
    DataSchema schema =
        DataSchema.builder()
                  .withDataSource("datasource")
                  .withTimestamp(new TimestampSpec(null, null, null))
                  .withDimensions(dimensionsSpec)
                  .withGranularity(
                      new UniformGranularitySpec(Granularities.MINUTE, Granularities.NONE, null)
                  )
                  .build();

    SeekableStreamIndexTaskTuningConfig tuningConfig = Mockito.mock(SeekableStreamIndexTaskTuningConfig.class);
    SeekableStreamIndexTaskIOConfig<String, String> ioConfig = Mockito.mock(SeekableStreamIndexTaskIOConfig.class);
    SeekableStreamStartSequenceNumbers<String, String> sequenceNumbers = Mockito.mock(SeekableStreamStartSequenceNumbers.class);
    SeekableStreamEndSequenceNumbers<String, String> endSequenceNumbers = Mockito.mock(SeekableStreamEndSequenceNumbers.class);

    DateTime now = DateTimes.nowUtc();

    Mockito.when(ioConfig.getRefreshRejectionPeriodsInMinutes()).thenReturn(null);
    // min max time not populated.
    Mockito.when(ioConfig.getMaximumMessageTime()).thenReturn(Optional.absent());
    Mockito.when(ioConfig.getMinimumMessageTime()).thenReturn(Optional.absent());
    Mockito.when(ioConfig.getInputFormat()).thenReturn(new JsonInputFormat(null, null, null, null, null));
    Mockito.when(ioConfig.getStartSequenceNumbers()).thenReturn(sequenceNumbers);
    Mockito.when(ioConfig.getEndSequenceNumbers()).thenReturn(endSequenceNumbers);

    Mockito.when(endSequenceNumbers.getPartitionSequenceNumberMap()).thenReturn(ImmutableMap.of());
    Mockito.when(sequenceNumbers.getStream()).thenReturn("test");

    Mockito.when(task.getDataSchema()).thenReturn(schema);
    Mockito.when(task.getIOConfig()).thenReturn(ioConfig);
    Mockito.when(task.getTuningConfig()).thenReturn(tuningConfig);
    TestasbleSeekableStreamIndexTaskRunner runner = new TestasbleSeekableStreamIndexTaskRunner(task, null, null, LockGranularity.TIME_CHUNK);

    Assert.assertTrue(runner.withinMinMaxRecordTime(row));

    Mockito.when(row.getTimestamp()).thenReturn(now.minusHours(2).minusMinutes(1));
    Assert.assertTrue(runner.withinMinMaxRecordTime(row));

    Mockito.when(row.getTimestamp()).thenReturn(now.plusHours(2).plusMinutes(1));
    Assert.assertTrue(runner.withinMinMaxRecordTime(row));
  }

  static class TestasbleSeekableStreamIndexTaskRunner extends SeekableStreamIndexTaskRunner
  {
    public TestasbleSeekableStreamIndexTaskRunner(
        SeekableStreamIndexTask task,
        @Nullable InputRowParser parser,
        AuthorizerMapper authorizerMapper,
        LockGranularity lockGranularityToUse
    )
    {
      super(task, parser, authorizerMapper, lockGranularityToUse);
    }

    @Override
    protected boolean isEndOfShard(Object seqNum)
    {
      return false;
    }

    @Nullable
    @Override
    protected TreeMap<Integer, Map> getCheckPointsFromContext(TaskToolbox toolbox, String checkpointsString)
    {
      return null;
    }

    @Override
    protected Object getNextStartOffset(Object sequenceNumber)
    {
      return null;
    }

    @Override
    protected SeekableStreamEndSequenceNumbers deserializePartitionsFromMetadata(ObjectMapper mapper, Object object)
    {
      return null;
    }

    @Override
    protected List<OrderedPartitionableRecord> getRecords(RecordSupplier recordSupplier, TaskToolbox toolbox)
    {
      return null;
    }

    @Override
    protected SeekableStreamDataSourceMetadata createDataSourceMetadata(SeekableStreamSequenceNumbers partitions)
    {
      return null;
    }

    @Override
    protected OrderedSequenceNumber createSequenceNumber(Object sequenceNumber)
    {
      return null;
    }

    @Override
    protected boolean isEndOffsetExclusive()
    {
      return false;
    }

    @Override
    protected TypeReference<List<SequenceMetadata>> getSequenceMetadataTypeReference()
    {
      return null;
    }

    @Override
    protected void possiblyResetDataSourceMetadata(TaskToolbox toolbox, RecordSupplier recordSupplier, Set assignment)
    {

    }
  }
}
