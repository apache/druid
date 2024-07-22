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

package org.apache.druid.server.coordinator.compact;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.client.indexing.ClientCompactionTaskQuery;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

public class CompactionStatusTrackerTest
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();
  private static final DataSegment WIKI_SEGMENT
      = CreateDataSegments.ofDatasource(DS.WIKI).eachOfSizeInMb(100).get(0);

  private CompactionStatusTracker statusTracker;

  @Before
  public void setup()
  {
    statusTracker = new CompactionStatusTracker(MAPPER);
  }

  @Test
  public void testIntervalIsSkippedIfSegmentSizeExceedsLimit()
  {
    CompactionStatus status = statusTracker.computeCompactionStatus(
        SegmentsToCompact.from(Collections.singletonList(WIKI_SEGMENT)),
        createCompactionConfig(DS.WIKI, 100L)
    );
    Assert.assertTrue(status.isSkipped());
    Assert.assertEquals(
        "Total segment size[100000000] is larger than allowed inputSegmentSize[100]",
        status.getReason()
    );
  }

  @Test
  public void testIntervalIsSkippedIfRecentlySubmitted()
  {
    final SegmentsToCompact candidateSegments
        = SegmentsToCompact.from(Collections.singletonList(WIKI_SEGMENT));
    statusTracker.onTaskSubmitted(createCompactionTask("task1", DS.WIKI), candidateSegments);

    CompactionStatus status = statusTracker.computeCompactionStatus(
        candidateSegments,
        createCompactionConfig(DS.WIKI, null)
    );
    Assert.assertTrue(status.isSkipped());
    Assert.assertEquals(
        "Interval[2012-10-24T00:00:00.000Z/2012-10-25T00:00:00.000Z] was recently"
        + " submitted for compaction and has state[TASK_SUBMITTED].",
        status.getReason()
    );
  }

  @Test
  public void testIntervalIsSkippedIfRecentlyCompacted()
  {
    final SegmentsToCompact candidateSegments
        = SegmentsToCompact.from(Collections.singletonList(WIKI_SEGMENT));

    final String taskId = "task1";
    statusTracker.onTaskSubmitted(createCompactionTask(taskId, DS.WIKI), candidateSegments);
    statusTracker.onTaskFinished(taskId, TaskStatus.success(taskId));

    CompactionStatus status = statusTracker.computeCompactionStatus(
        candidateSegments,
        createCompactionConfig(DS.WIKI, null)
    );
    Assert.assertTrue(status.isSkipped());
    Assert.assertEquals(
        "Interval[2012-10-24T00:00:00.000Z/2012-10-25T00:00:00.000Z] was recently"
        + " submitted for compaction and has state[COMPACTED].",
        status.getReason()
    );
  }

  @Test
  public void testIntervalIsNotSkippedIfFailedOnce()
  {
    final SegmentsToCompact candidateSegments
        = SegmentsToCompact.from(Collections.singletonList(WIKI_SEGMENT));

    final String taskId = "task1";
    statusTracker.onTaskSubmitted(createCompactionTask(taskId, DS.WIKI), candidateSegments);
    statusTracker.onTaskFinished(taskId, TaskStatus.failure(taskId, "first error"));

    CompactionStatus status = statusTracker.computeCompactionStatus(
        candidateSegments,
        createCompactionConfig(DS.WIKI, null)
    );
    Assert.assertFalse(status.isComplete() || status.isSkipped());
    Assert.assertEquals("Not compacted yet", status.getReason());
  }

  @Test
  public void testIntervalIsSkippedIfFailedForMaxRetries()
  {
    final SegmentsToCompact candidateSegments
        = SegmentsToCompact.from(Collections.singletonList(WIKI_SEGMENT));
    final DataSourceCompactionConfig compactionConfig
        = createCompactionConfig(DS.WIKI, null);

    // Verify that interval remains eligible for compaction until max failure retries
    for (int i = 0; i < 4; ++i) {
      CompactionStatus status = statusTracker.computeCompactionStatus(
          candidateSegments,
          compactionConfig
      );
      Assert.assertFalse(status.isComplete() || status.isSkipped());
      Assert.assertEquals("Not compacted yet", status.getReason());

      final String taskId = "task_" + i;
      statusTracker.onTaskSubmitted(createCompactionTask(taskId, DS.WIKI), candidateSegments);
      statusTracker.onTaskFinished(taskId, TaskStatus.failure(taskId, "error number " + i));
    }

    CompactionStatus status = statusTracker.computeCompactionStatus(
        candidateSegments,
        createCompactionConfig(DS.WIKI, null)
    );
    Assert.assertTrue(status.isSkipped());
    Assert.assertEquals(
        "Interval[2012-10-24T00:00:00.000Z/2012-10-25T00:00:00.000Z] was recently"
        + " submitted for compaction and has state[FAILED_ALL_RETRIES].",
        status.getReason()
    );
  }

  @Test
  public void testIntervalBecomesEligibleAgainAfterMaxSkips()
  {
    final SegmentsToCompact candidateSegments
        = SegmentsToCompact.from(Collections.singletonList(WIKI_SEGMENT));

    final String taskId = "task1";
    statusTracker.onTaskSubmitted(createCompactionTask(taskId, DS.WIKI), candidateSegments);
    statusTracker.onTaskFinished(taskId, TaskStatus.success(taskId));

    // Verify that interval is skipped until skip count is exhausted
    for (int i = 0; i < 5; ++i) {
      CompactionStatus status = statusTracker.computeCompactionStatus(
          candidateSegments,
          createCompactionConfig(DS.WIKI, null)
      );
      Assert.assertTrue(status.isSkipped());
      Assert.assertEquals(
          "Interval[2012-10-24T00:00:00.000Z/2012-10-25T00:00:00.000Z] was recently"
          + " submitted for compaction and has state[COMPACTED].",
          status.getReason()
      );

      // Submit task for a different interval to decrement skip count
      final DataSegment segment
          = DataSegment.builder(WIKI_SEGMENT)
                       .interval(shift(WIKI_SEGMENT.getInterval(), Duration.standardDays(i + 1)))
                       .build();
      statusTracker.onTaskSubmitted(
          createCompactionTask("task_" + i, DS.WIKI),
          SegmentsToCompact.from(Collections.singletonList(segment))
      );
    }

    // Verify that interval is now eligible again
    CompactionStatus status = statusTracker.computeCompactionStatus(
        candidateSegments,
        createCompactionConfig(DS.WIKI, null)
    );
    Assert.assertFalse(status.isComplete() || status.isSkipped());
    Assert.assertEquals("Not compacted yet", status.getReason());
  }

  @Test
  public void testResetClearsAllStatuses()
  {

  }

  private static Interval shift(Interval interval, Duration duration)
  {
    return interval.withEnd(interval.getEnd().plus(duration))
                   .withStart(interval.getStart().plus(duration));
  }

  private DataSourceCompactionConfig createCompactionConfig(
      String datasource,
      Long inputSegmentSizeBytes
  )
  {
    return new DataSourceCompactionConfig(
        datasource,
        null,
        inputSegmentSizeBytes,
        null,
        null,
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

  private ClientCompactionTaskQuery createCompactionTask(
      String taskId,
      String datasource
  )
  {
    return new ClientCompactionTaskQuery(
        taskId,
        datasource,
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

  private static class DS
  {
    static final String WIKI = "wiki";
  }
}
