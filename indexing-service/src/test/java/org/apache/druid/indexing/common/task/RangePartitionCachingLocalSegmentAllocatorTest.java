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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.LockListAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.task.batch.parallel.distribution.Partitions;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.SingleDimensionShardSpec;
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
  private static final Interval INTERVAL_FREQUENT_MID = Intervals.utc(3000, 4000);
  private static final Interval INTERVAL_FREQUENT_MAX = Intervals.utc(5000, 6000);
  private static final Map<Interval, String> INTERVAL_TO_VERSION = ImmutableMap.of(
      INTERVAL_EMPTY, "version-empty",
      INTERVAL_SINGLETON, "version-singleton",
      INTERVAL_NORMAL, "version-normal",
      INTERVAL_FREQUENT_MID, "version-frequent-mid",
      INTERVAL_FREQUENT_MAX, "version-frequent-max"
  );
  private static final String PARTITION0 = "0";
  private static final String PARTITION5 = "5";
  private static final String PARTITION9 = "9";
  private static final Partitions EMPTY_PARTITIONS = new Partitions();
  private static final Partitions SINGLETON_PARTITIONS = new Partitions(PARTITION0, PARTITION0);
  private static final Partitions NORMAL_PARTITIONS = new Partitions(PARTITION0, PARTITION5, PARTITION9);
  private static final Partitions FREQUENT_MID_PARTITIONS = new Partitions(
      PARTITION0,
      PARTITION5,
      PARTITION5,
      PARTITION9
  );
  private static final Partitions FREQUENT_MAX_PARTITIONS = new Partitions(
      PARTITION0,
      PARTITION5,
      PARTITION9,
      PARTITION9
  );

  private static final Map<Interval, Partitions> INTERVAL_TO_PARTITONS = ImmutableMap.of(
      INTERVAL_EMPTY, EMPTY_PARTITIONS,
      INTERVAL_SINGLETON, SINGLETON_PARTITIONS,
      INTERVAL_NORMAL, NORMAL_PARTITIONS,
      INTERVAL_FREQUENT_MID, FREQUENT_MID_PARTITIONS,
      INTERVAL_FREQUENT_MAX, FREQUENT_MAX_PARTITIONS
  );

  private RangePartitionCachingLocalSegmentAllocator target;

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
    target = new RangePartitionCachingLocalSegmentAllocator(
        toolbox,
        TASKID,
        SUPERVISOR_TASKID,
        DATASOURCE,
        PARTITION_DIMENSION,
        INTERVAL_TO_PARTITONS
    );
  }

  @Test
  public void failsIfAllocateFromEmptyInterval()
  {
    Interval interval = INTERVAL_EMPTY;
    InputRow row = createInputRow(interval, PARTITION9);

    exception.expect(IllegalStateException.class);
    exception.expectMessage("Failed to get shardSpec");

    String sequenceName = target.getSequenceName(interval, row);
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
  public void allocatesCorrectShardSpecsForLastPartitionWithoutFrequentValue()
  {
    Interval interval = INTERVAL_NORMAL;
    InputRow row = createInputRow(interval, PARTITION9);
    testAllocate(row, interval, INTERVAL_TO_PARTITONS.get(interval).size() - 1, null);
  }

  @Test
  public void allocatesCorrectShardSpecsForLastPartitionWithFrequentMid()
  {
    Interval interval = INTERVAL_FREQUENT_MID;
    InputRow row = createInputRow(interval, PARTITION9);
    Partitions partitions = INTERVAL_TO_PARTITONS.get(interval);
    testAllocate(row, interval, partitions.size() - 2, partitions.get(partitions.size() - 1), null);
  }

  @Test
  public void allocatesCorrectShardSpecsForLastPartitionWithFrequentMax()
  {
    Interval interval = INTERVAL_FREQUENT_MAX;
    InputRow row = createInputRow(interval, PARTITION9);
    testAllocate(row, interval, INTERVAL_TO_PARTITONS.get(interval).size() - 2, null);
  }

  @SuppressWarnings("SameParameterValue")
  private void testAllocate(InputRow row, Interval interval, int partitionNum)
  {
    testAllocate(row, interval, partitionNum, INTERVAL_TO_PARTITONS.get(interval).get(partitionNum + 1));
  }

  private void testAllocate(InputRow row, Interval interval, int partitionNum, @Nullable String partitionEnd)
  {
    testAllocate(row, interval, partitionNum, INTERVAL_TO_PARTITONS.get(interval).get(partitionNum), partitionEnd);
  }

  private void testAllocate(
      InputRow row,
      Interval interval,
      int partitionNum,
      String partitionStart,
      @Nullable String partitionEnd
  )
  {
    String sequenceName = target.getSequenceName(interval, row);
    SegmentIdWithShardSpec segmentIdWithShardSpec = allocate(row, sequenceName);

    Assert.assertEquals(
        SegmentId.of(DATASOURCE, interval, INTERVAL_TO_VERSION.get(interval), partitionNum),
        segmentIdWithShardSpec.asSegmentId()
    );
    SingleDimensionShardSpec shardSpec = (SingleDimensionShardSpec) segmentIdWithShardSpec.getShardSpec();
    Assert.assertEquals(PARTITION_DIMENSION, shardSpec.getDimension());
    Assert.assertEquals(partitionNum, shardSpec.getPartitionNum());
    Assert.assertEquals(partitionStart, shardSpec.getStart());
    Assert.assertEquals(partitionEnd, shardSpec.getEnd());
  }

  private SegmentIdWithShardSpec allocate(InputRow row, String sequenceName)
  {
    try {
      return target.allocate(row, sequenceName, null, false);
    }
    catch (IOException e) {
      throw new UncheckedIOException(e);
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
