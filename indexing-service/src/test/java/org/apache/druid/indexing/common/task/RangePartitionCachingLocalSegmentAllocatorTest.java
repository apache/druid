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

package org.apache.druid.indexing.common.task;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.indexer.partitions.SingleDimensionPartitionsSpec;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.LockListAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.task.batch.partition.RangePartitionAnalysis;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.realtime.appenderator.SegmentAllocator;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.PartitionBoundaries;
import org.apache.druid.timeline.partition.RangeBucketShardSpec;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RangePartitionCachingLocalSegmentAllocatorTest
{
  private static final String DATASOURCE = "datasource";
  private static final String TASKID = "taskid";
  private static final String SUPERVISOR_TASKID = "supervisor-taskid";
  private static final String PARTITION_DIMENSION = "dimension";
  private static final Interval INTERVAL_EMPTY = Intervals.utc(0, 1000);
  private static final Interval INTERVAL_SINGLETON = Intervals.utc(1000, 2000);
  private static final Interval INTERVAL_NORMAL = Intervals.utc(2000, 3000);
  private static final Map<Interval, String> INTERVAL_TO_VERSION = ImmutableMap.of(
      INTERVAL_EMPTY, "version-empty",
      INTERVAL_SINGLETON, "version-singleton",
      INTERVAL_NORMAL, "version-normal"
  );
  private static final String PARTITION0 = "0";
  private static final String PARTITION5 = "5";
  private static final String PARTITION9 = "9";
  private static final PartitionBoundaries EMPTY_PARTITIONS = new PartitionBoundaries();
  private static final PartitionBoundaries SINGLETON_PARTITIONS = new PartitionBoundaries(PARTITION0, PARTITION0);
  private static final PartitionBoundaries NORMAL_PARTITIONS = new PartitionBoundaries(
      PARTITION0,
      PARTITION5,
      PARTITION9
  );

  private static final Map<Interval, PartitionBoundaries> INTERVAL_TO_PARTITONS = ImmutableMap.of(
      INTERVAL_EMPTY, EMPTY_PARTITIONS,
      INTERVAL_SINGLETON, SINGLETON_PARTITIONS,
      INTERVAL_NORMAL, NORMAL_PARTITIONS
  );

  private SegmentAllocator target;
  private SequenceNameFunction sequenceNameFunction;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Before
  public void setup() throws IOException
  {
    TaskToolbox toolbox = createToolbox(
        INTERVAL_TO_VERSION.keySet()
                           .stream()
                           .map(RangePartitionCachingLocalSegmentAllocatorTest::createTaskLock)
                           .collect(Collectors.toList())
    );
    final RangePartitionAnalysis partitionAnalysis = new RangePartitionAnalysis(
        new SingleDimensionPartitionsSpec(null, 1, PARTITION_DIMENSION, false)
    );
    INTERVAL_TO_PARTITONS.forEach(partitionAnalysis::updateBucket);
    target = SegmentAllocators.forNonLinearPartitioning(
        toolbox,
        DATASOURCE,
        TASKID,
        new UniformGranularitySpec(Granularities.HOUR, Granularities.NONE, ImmutableList.of()),
        new SupervisorTaskAccessWithNullClient(SUPERVISOR_TASKID),
        partitionAnalysis
    );
    sequenceNameFunction = ((CachingLocalSegmentAllocator) target).getSequenceNameFunction();
  }

  @Test
  public void failsIfAllocateFromEmptyInterval()
  {
    Interval interval = INTERVAL_EMPTY;
    InputRow row = createInputRow(interval, PARTITION9);

    exception.expect(IllegalStateException.class);
    exception.expectMessage("Failed to get shardSpec");

    String sequenceName = sequenceNameFunction.getSequenceName(interval, row);
    allocate(row, sequenceName);
  }

  @Test
  public void allocatesCorrectShardSpecsForSingletonPartitions()
  {
    Interval interval = INTERVAL_SINGLETON;
    InputRow row = createInputRow(interval, PARTITION9);
    testAllocate(row, interval, 0, null);
  }


  @Test
  public void allocatesCorrectShardSpecsForFirstPartition()
  {
    Interval interval = INTERVAL_NORMAL;
    InputRow row = createInputRow(interval, PARTITION0);
    testAllocate(row, interval, 0);
  }

  @Test
  public void allocatesCorrectShardSpecsForLastPartition()
  {
    Interval interval = INTERVAL_NORMAL;
    InputRow row = createInputRow(interval, PARTITION9);
    int partitionNum = INTERVAL_TO_PARTITONS.get(interval).size() - 2;
    testAllocate(row, interval, partitionNum, null);
  }

  @Test
  public void getSequenceName()
  {
    // getSequenceName_forIntervalAndRow_shouldUseISOFormatAndPartitionNumForRow
    Interval interval = INTERVAL_NORMAL;
    InputRow row = createInputRow(interval, PARTITION9);
    String sequenceName = sequenceNameFunction.getSequenceName(interval, row);
    String expectedSequenceName = StringUtils.format("%s_%s_%d", TASKID, interval, 1);
    Assert.assertEquals(expectedSequenceName, sequenceName);
  }

  @SuppressWarnings("SameParameterValue")
  private void testAllocate(InputRow row, Interval interval, int bucketId)
  {
    String partitionEnd = getPartitionEnd(interval, bucketId);
    testAllocate(row, interval, bucketId, partitionEnd);
  }

  @Nullable
  private static String getPartitionEnd(Interval interval, int bucketId)
  {
    PartitionBoundaries partitions = INTERVAL_TO_PARTITONS.get(interval);
    boolean isLastPartition = (bucketId + 1) == partitions.size();
    return isLastPartition ? null : partitions.get(bucketId + 1);
  }

  private void testAllocate(InputRow row, Interval interval, int bucketId, @Nullable String partitionEnd)
  {
    String partitionStart = getPartitionStart(interval, bucketId);
    testAllocate(row, interval, bucketId, partitionStart, partitionEnd);
  }

  @Nullable
  private static String getPartitionStart(Interval interval, int bucketId)
  {
    boolean isFirstPartition = bucketId == 0;
    return isFirstPartition ? null : INTERVAL_TO_PARTITONS.get(interval).get(bucketId);
  }

  private void testAllocate(
      InputRow row,
      Interval interval,
      int bucketId,
      @Nullable String partitionStart,
      @Nullable String partitionEnd
  )
  {
    String sequenceName = sequenceNameFunction.getSequenceName(interval, row);
    SegmentIdWithShardSpec segmentIdWithShardSpec = allocate(row, sequenceName);

    Assert.assertEquals(
        SegmentId.of(DATASOURCE, interval, INTERVAL_TO_VERSION.get(interval), bucketId),
        segmentIdWithShardSpec.asSegmentId()
    );
    RangeBucketShardSpec shardSpec = (RangeBucketShardSpec) segmentIdWithShardSpec.getShardSpec();
    Assert.assertEquals(PARTITION_DIMENSION, shardSpec.getDimension());
    Assert.assertEquals(bucketId, shardSpec.getBucketId());
    Assert.assertEquals(partitionStart, shardSpec.getStart());
    Assert.assertEquals(partitionEnd, shardSpec.getEnd());
  }

  private SegmentIdWithShardSpec allocate(InputRow row, String sequenceName)
  {
    try {
      return target.allocate(row, sequenceName, null, false);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static TaskToolbox createToolbox(List<TaskLock> taskLocks)
  {
    TaskToolbox toolbox = EasyMock.mock(TaskToolbox.class);
    EasyMock.expect(toolbox.getTaskActionClient()).andStubReturn(createTaskActionClient(taskLocks));
    EasyMock.replay(toolbox);
    return toolbox;
  }

  private static TaskActionClient createTaskActionClient(List<TaskLock> taskLocks)
  {
    try {
      TaskActionClient taskActionClient = EasyMock.mock(TaskActionClient.class);
      EasyMock.expect(taskActionClient.submit(EasyMock.anyObject(LockListAction.class))).andStubReturn(taskLocks);
      EasyMock.replay(taskActionClient);
      return taskActionClient;
    }
    catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static TaskLock createTaskLock(Interval interval)
  {
    TaskLock taskLock = EasyMock.mock(TaskLock.class);
    EasyMock.expect(taskLock.getInterval()).andStubReturn(interval);
    EasyMock.expect(taskLock.getVersion()).andStubReturn(INTERVAL_TO_VERSION.get(interval));
    EasyMock.replay(taskLock);
    return taskLock;
  }

  private static InputRow createInputRow(Interval interval, String dimensionValue)
  {
    long timestamp = interval.getStartMillis();
    InputRow inputRow = EasyMock.mock(InputRow.class);
    EasyMock.expect(inputRow.getTimestamp()).andStubReturn(DateTimes.utc(timestamp));
    EasyMock.expect(inputRow.getTimestampFromEpoch()).andStubReturn(timestamp);
    EasyMock.expect(inputRow.getDimension(PARTITION_DIMENSION))
            .andStubReturn(Collections.singletonList(dimensionValue));
    EasyMock.replay(inputRow);
    return inputRow;
  }
}
