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

package org.apache.druid.server.compaction;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.client.indexing.ClientCompactionTaskQuery;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.timeline.DataSegment;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

public class CompactionStatusTrackerTest
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();
  private static final DataSegment WIKI_SEGMENT
      = CreateDataSegments.ofDatasource(TestDataSource.WIKI).eachOfSizeInMb(100).get(0);

  private CompactionStatusTracker statusTracker;

  @Before
  public void setup()
  {
    statusTracker = new CompactionStatusTracker(MAPPER);
  }

  @Test
  public void testGetLatestTaskStatusForSubmittedTask()
  {
    final CompactionCandidate candidateSegments
        = CompactionCandidate.from(Collections.singletonList(WIKI_SEGMENT));
    statusTracker.onTaskSubmitted(createCompactionTask("task1"), candidateSegments);

    CompactionTaskStatus status = statusTracker.getLatestTaskStatus(candidateSegments);
    Assert.assertEquals(TaskState.RUNNING, status.getState());
  }

  @Test
  public void testGetLatestTaskStatusForSuccessfulTask()
  {
    final CompactionCandidate candidateSegments
        = CompactionCandidate.from(Collections.singletonList(WIKI_SEGMENT));
    statusTracker.onTaskSubmitted(createCompactionTask("task1"), candidateSegments);
    statusTracker.onTaskFinished("task1", TaskStatus.success("task1"));

    CompactionTaskStatus status = statusTracker.getLatestTaskStatus(candidateSegments);
    Assert.assertEquals(TaskState.SUCCESS, status.getState());
  }

  @Test
  public void testGetLatestTaskStatusForFailedTask()
  {
    final CompactionCandidate candidateSegments
        = CompactionCandidate.from(Collections.singletonList(WIKI_SEGMENT));
    statusTracker.onTaskSubmitted(createCompactionTask("task1"), candidateSegments);
    statusTracker.onTaskFinished("task1", TaskStatus.failure("task1", "some failure"));

    CompactionTaskStatus status = statusTracker.getLatestTaskStatus(candidateSegments);
    Assert.assertEquals(TaskState.FAILED, status.getState());
    Assert.assertEquals(1, status.getNumConsecutiveFailures());
  }

  @Test
  public void testGetLatestTaskStatusForRepeatedlyFailingTask()
  {
    final CompactionCandidate candidateSegments
        = CompactionCandidate.from(Collections.singletonList(WIKI_SEGMENT));

    statusTracker.onTaskSubmitted(createCompactionTask("task1"), candidateSegments);
    statusTracker.onTaskFinished("task1", TaskStatus.failure("task1", "some failure"));

    statusTracker.onTaskSubmitted(createCompactionTask("task2"), candidateSegments);
    CompactionTaskStatus status = statusTracker.getLatestTaskStatus(candidateSegments);
    Assert.assertEquals(TaskState.RUNNING, status.getState());
    Assert.assertEquals(1, status.getNumConsecutiveFailures());

    statusTracker.onTaskFinished("task2", TaskStatus.failure("task2", "second failure"));

    status = statusTracker.getLatestTaskStatus(candidateSegments);
    Assert.assertEquals(TaskState.FAILED, status.getState());
    Assert.assertEquals(2, status.getNumConsecutiveFailures());
  }

  @Test
  public void testComputeCompactionStatusForSuccessfulTask()
  {
    final DataSourceCompactionConfig compactionConfig
        = DataSourceCompactionConfig.builder().forDataSource(TestDataSource.WIKI).build();
    final NewestSegmentFirstPolicy policy = new NewestSegmentFirstPolicy(null);
    final CompactionCandidate candidateSegments
        = CompactionCandidate.from(Collections.singletonList(WIKI_SEGMENT));

    // Verify that interval is originally eligible for compaction
    CompactionStatus status
        = statusTracker.computeCompactionStatus(candidateSegments, compactionConfig, policy);
    Assert.assertEquals(CompactionStatus.State.PENDING, status.getState());
    Assert.assertEquals("not compacted yet", status.getReason());

    // Verify that interval is skipped for compaction after task has finished
    statusTracker.onSegmentTimelineUpdated(DateTimes.nowUtc().minusMinutes(1));
    statusTracker.onTaskSubmitted(createCompactionTask("task1"), candidateSegments);
    statusTracker.onTaskFinished("task1", TaskStatus.success("task1"));

    status = statusTracker.computeCompactionStatus(candidateSegments, compactionConfig, policy);
    Assert.assertEquals(CompactionStatus.State.SKIPPED, status.getState());
    Assert.assertEquals(
        "Segment timeline not updated since last compaction task succeeded",
        status.getReason()
    );

    // Verify that interval becomes eligible again after timeline has been updated
    statusTracker.onSegmentTimelineUpdated(DateTimes.nowUtc());
    status = statusTracker.computeCompactionStatus(candidateSegments, compactionConfig, policy);
    Assert.assertEquals(CompactionStatus.State.PENDING, status.getState());
  }

  private ClientCompactionTaskQuery createCompactionTask(
      String taskId
  )
  {
    return new ClientCompactionTaskQuery(
        taskId,
        TestDataSource.WIKI,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );
  }
}
