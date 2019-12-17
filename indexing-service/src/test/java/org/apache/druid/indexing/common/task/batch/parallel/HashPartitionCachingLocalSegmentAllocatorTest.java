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

package org.apache.druid.indexing.common.task.batch.parallel;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.LockListAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.task.HashPartitionCachingLocalSegmentAllocator;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpecFactory;
import org.apache.druid.timeline.partition.ShardSpecFactory;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class HashPartitionCachingLocalSegmentAllocatorTest
{
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String DATASOURCE = "datasource";
  private static final String TASKID = "taskid";
  private static final String SUPERVISOR_TASKID = "supervisor-taskid";
  private static final Interval INTERVAL = Intervals.utc(0, 1000);
  private static final String VERSION = "version";
  private static final String DIMENSION = "dim";
  private static final List<String> PARTITION_DIMENSIONS = ImmutableList.of(DIMENSION);
  private static final int NUM_PARTITONS = 1;
  private static final ShardSpecFactory SHARD_SPEC_FACTORY = new HashBasedNumberedShardSpecFactory(
      PARTITION_DIMENSIONS,
      NUM_PARTITONS
  );
  private static final int PARTITION_NUM = 0;
  private static final Map<Interval, Pair<ShardSpecFactory, Integer>> ALLOCATE_SPEC = ImmutableMap.of(
      INTERVAL, Pair.of(SHARD_SPEC_FACTORY, NUM_PARTITONS)
  );

  private HashPartitionCachingLocalSegmentAllocator target;

  @Before
  public void setup() throws IOException
  {
    TaskToolbox toolbox = createToolbox();
    target = new HashPartitionCachingLocalSegmentAllocator(
        toolbox,
        TASKID,
        SUPERVISOR_TASKID,
        DATASOURCE,
        ALLOCATE_SPEC
    );
  }

  @Test
  public void allocatesCorrectShardSpec() throws IOException
  {
    InputRow row = createInputRow();

    String sequenceName = target.getSequenceName(INTERVAL, row);
    SegmentIdWithShardSpec segmentIdWithShardSpec = target.allocate(row, sequenceName, null, false);

    Assert.assertEquals(
        SegmentId.of(DATASOURCE, INTERVAL, VERSION, PARTITION_NUM),
        segmentIdWithShardSpec.asSegmentId()
    );
    HashBasedNumberedShardSpec shardSpec = (HashBasedNumberedShardSpec) segmentIdWithShardSpec.getShardSpec();
    Assert.assertEquals(PARTITION_DIMENSIONS, shardSpec.getPartitionDimensions());
    Assert.assertEquals(NUM_PARTITONS, shardSpec.getPartitions());
    Assert.assertEquals(PARTITION_NUM, shardSpec.getPartitionNum());
  }


  @Test
  public void test_getSequenceName_forIntervalAndRow_shouldUseISOFormatAndPartitionNumForRow()
  {
    InputRow row = createInputRow();
    String sequenceName = target.getSequenceName(INTERVAL, row);
    String expectedSequenceName = StringUtils.format("%s_%s_%d", TASKID, INTERVAL, PARTITION_NUM);
    Assert.assertEquals(expectedSequenceName, sequenceName);
  }

  private static TaskToolbox createToolbox()
  {
    TaskToolbox toolbox = EasyMock.mock(TaskToolbox.class);
    EasyMock.expect(toolbox.getTaskActionClient()).andStubReturn(createTaskActionClient());
    EasyMock.expect(toolbox.getJsonMapper()).andStubReturn(OBJECT_MAPPER);
    EasyMock.replay(toolbox);
    return toolbox;
  }

  private static TaskActionClient createTaskActionClient()
  {
    List<TaskLock> taskLocks = Collections.singletonList(createTaskLock());

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

  private static TaskLock createTaskLock()
  {
    TaskLock taskLock = EasyMock.mock(TaskLock.class);
    EasyMock.expect(taskLock.getInterval()).andStubReturn(INTERVAL);
    EasyMock.expect(taskLock.getVersion()).andStubReturn(VERSION);
    EasyMock.replay(taskLock);
    return taskLock;
  }

  private static InputRow createInputRow()
  {
    long timestamp = INTERVAL.getStartMillis();
    InputRow inputRow = EasyMock.mock(InputRow.class);
    EasyMock.expect(inputRow.getTimestamp()).andStubReturn(DateTimes.utc(timestamp));
    EasyMock.expect(inputRow.getTimestampFromEpoch()).andStubReturn(timestamp);
    EasyMock.expect(inputRow.getDimension(DIMENSION)).andStubReturn(Collections.singletonList(DIMENSION));
    EasyMock.replay(inputRow);
    return inputRow;
  }
}
