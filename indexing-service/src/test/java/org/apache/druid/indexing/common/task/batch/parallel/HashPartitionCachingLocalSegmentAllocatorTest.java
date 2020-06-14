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
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.LockListAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.task.CachingLocalSegmentAllocator;
import org.apache.druid.indexing.common.task.SegmentAllocators;
import org.apache.druid.indexing.common.task.SequenceNameFunction;
import org.apache.druid.indexing.common.task.SupervisorTaskAccessWithNullClient;
import org.apache.druid.indexing.common.task.batch.partition.HashPartitionAnalysis;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.realtime.appenderator.SegmentAllocator;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.HashBucketShardSpec;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;

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
  private static final int PARTITION_NUM = 0;
  private static final HashedPartitionsSpec PARTITIONS_SPEC = new HashedPartitionsSpec(
      null,
      null,
      Collections.singletonList(DIMENSION)
  );

  private SegmentAllocator target;
  private SequenceNameFunction sequenceNameFunction;

  @Before
  public void setup() throws IOException
  {
    TaskToolbox toolbox = createToolbox();
    HashPartitionAnalysis partitionAnalysis = new HashPartitionAnalysis(PARTITIONS_SPEC);
    partitionAnalysis.updateBucket(INTERVAL, NUM_PARTITONS);
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
  public void allocatesCorrectShardSpec() throws IOException
  {
    InputRow row = createInputRow();

    String sequenceName = sequenceNameFunction.getSequenceName(INTERVAL, row);
    SegmentIdWithShardSpec segmentIdWithShardSpec = target.allocate(row, sequenceName, null, false);

    Assert.assertEquals(
        SegmentId.of(DATASOURCE, INTERVAL, VERSION, PARTITION_NUM),
        segmentIdWithShardSpec.asSegmentId()
    );
    HashBucketShardSpec shardSpec = (HashBucketShardSpec) segmentIdWithShardSpec.getShardSpec();
    Assert.assertEquals(PARTITION_DIMENSIONS, shardSpec.getPartitionDimensions());
    Assert.assertEquals(NUM_PARTITONS, shardSpec.getNumBuckets());
    Assert.assertEquals(PARTITION_NUM, shardSpec.getBucketId());
  }

  @Test
  public void getSequenceName()
  {
    // getSequenceName_forIntervalAndRow_shouldUseISOFormatAndPartitionNumForRow
    InputRow row = createInputRow();
    String sequenceName = sequenceNameFunction.getSequenceName(INTERVAL, row);
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
    final long timestamp = INTERVAL.getStartMillis();
    return new MapBasedInputRow(
        timestamp,
        Collections.singletonList(DIMENSION),
        ImmutableMap.of(DIMENSION, 1)
    );
  }
}
