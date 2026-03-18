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
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class CompactionStatusDetailedStatsTest
{
  private static final String DATASOURCE = TestDataSource.WIKI;

  private static final DataSegment SEGMENT_1 = DataSegment.builder(
      SegmentId.of(DATASOURCE, Intervals.of("2020-01-01/2020-01-02"), "v1", 0)
  ).size(100L).totalRows(10).build();

  private static final DataSegment SEGMENT_2 = DataSegment.builder(
      SegmentId.of(DATASOURCE, Intervals.of("2020-01-01/2020-01-02"), "v1", 1)
  ).size(200L).totalRows(20).build();

  private final ObjectMapper objectMapper = new DefaultObjectMapper();

  @Test
  public void testRecordPendingStatus()
  {
    CompactionStatusDetailedStats stats = new CompactionStatusDetailedStats();

    CompactionStatistics compactionStats = CompactionStatistics.create(300L, 30L, 2, 1);
    CompactionStatistics uncompactedStats = CompactionStatistics.create(300L, 30L, 2, 1);
    CompactionCandidate candidate = CompactionCandidate.from(
        Arrays.asList(SEGMENT_1, SEGMENT_2),
        null,
        CompactionStatus.pending(compactionStats, uncompactedStats, Arrays.asList(SEGMENT_1, SEGMENT_2), "Not compacted yet")
    );

    stats.recordCompactionStatus(candidate, null);

    Map<CompactionStatus.State, Table> result = stats.getCompactionStates();
    Assert.assertEquals(1, result.size());

    Table pendingTable = result.get(CompactionStatus.State.PENDING);
    Assert.assertEquals(1, pendingTable.getRows().size());
    Assert.assertEquals(
        Arrays.asList(DATASOURCE, Intervals.of("2020-01-01/2020-01-02"), 2, 300L, 30L, 2L, 300L, 30L, "Not compacted yet", null, null),
        pendingTable.getRows().get(0)
    );
  }

  @Test
  public void testRecordSkippedStatus()
  {
    CompactionStatusDetailedStats stats = new CompactionStatusDetailedStats();

    CompactionCandidate candidate = CompactionCandidate.from(
        List.of(SEGMENT_1),
        null,
        CompactionStatus.skipped("Interval locked")
    );

    stats.recordCompactionStatus(candidate, "Interval locked");

    Map<CompactionStatus.State, Table> result = stats.getCompactionStates();
    Assert.assertEquals(1, result.size());

    Table skippedTable = result.get(CompactionStatus.State.SKIPPED);
    Assert.assertEquals(1, skippedTable.getRows().size());
    Assert.assertEquals(
        Arrays.asList(DATASOURCE, Intervals.of("2020-01-01/2020-01-02"), 1, 100L, 10L, 0L, 0L, null, "Interval locked", null, "Interval locked"),
        skippedTable.getRows().get(0)
    );
  }

  @Test
  public void testRecordCompletedStatus()
  {
    CompactionStatusDetailedStats stats = new CompactionStatusDetailedStats();

    CompactionStatistics compactionStats = CompactionStatistics.create(300L, 30L, 2, 1);
    CompactionStatistics uncompactedStats = CompactionStatistics.create(0L, 0L, 0, 0);
    CompactionCandidate candidate = CompactionCandidate.from(
        Arrays.asList(SEGMENT_1, SEGMENT_2),
        null,
        CompactionStatus.complete(compactionStats, uncompactedStats)
    );

    stats.recordCompactionStatus(candidate, null);

    Map<CompactionStatus.State, Table> result = stats.getCompactionStates();
    Assert.assertEquals(1, result.size());

    Table completeTable = result.get(CompactionStatus.State.COMPLETE);
    Assert.assertEquals(1, completeTable.getRows().size());
    Assert.assertEquals(
        Arrays.asList(DATASOURCE, Intervals.of("2020-01-01/2020-01-02"), 2, 300L, 30L, 0L, 0L, 0L, null, null, null),
        completeTable.getRows().get(0)
    );
  }

  @Test
  public void testRecordSubmittedTask()
  {
    CompactionStatusDetailedStats stats = new CompactionStatusDetailedStats();

    CompactionStatistics compactionStats = CompactionStatistics.create(300L, 30L, 2, 1);
    CompactionStatistics uncompactedStats = CompactionStatistics.create(100L, 10L, 1, 1);
    CompactionCandidate candidate = CompactionCandidate.from(
        Arrays.asList(SEGMENT_1, SEGMENT_2),
        null,
        CompactionStatus.pending(compactionStats, uncompactedStats, List.of(SEGMENT_1), "Task submitted")
    );

    stats.recordSubmittedTask(candidate, CompactionMode.ALL_SEGMENTS);

    Map<CompactionStatus.State, Table> result = stats.getCompactionStates();
    Assert.assertEquals(1, result.size());

    Table runningTable = result.get(CompactionStatus.State.RUNNING);
    Assert.assertEquals(1, runningTable.getRows().size());
    // Note: uncompactedBytes column is missing in recordSubmittedTask implementation (bug)
    Assert.assertEquals(
        Arrays.asList(DATASOURCE, Intervals.of("2020-01-01/2020-01-02"), 2, 300L, 30L, 1L, 10L, "Task submitted", CompactionMode.ALL_SEGMENTS, null),
        runningTable.getRows().get(0)
    );
  }

  @Test
  public void testMultipleStates()
  {
    CompactionStatusDetailedStats stats = new CompactionStatusDetailedStats();

    CompactionStatistics stats1 = CompactionStatistics.create(100L, 10L, 1, 1);
    CompactionStatistics uncompacted1 = CompactionStatistics.create(100L, 10L, 1, 1);
    CompactionCandidate pending = CompactionCandidate.from(
        List.of(SEGMENT_1),
        null,
        CompactionStatus.pending(stats1, uncompacted1, List.of(SEGMENT_1), "Not yet compacted")
    );
    stats.recordCompactionStatus(pending, null);

    CompactionCandidate skipped = CompactionCandidate.from(
        List.of(SEGMENT_2),
        null,
        CompactionStatus.skipped("Locked")
    );
    stats.recordCompactionStatus(skipped, "Locked");

    Map<CompactionStatus.State, Table> result = stats.getCompactionStates();
    Assert.assertEquals(2, result.size());
    Assert.assertTrue(result.containsKey(CompactionStatus.State.PENDING));
    Assert.assertTrue(result.containsKey(CompactionStatus.State.SKIPPED));
  }

  @Test
  public void testEmptyStatesNotIncluded()
  {
    CompactionStatusDetailedStats stats = new CompactionStatusDetailedStats();

    Map<CompactionStatus.State, Table> result = stats.getCompactionStates();
    Assert.assertEquals(0, result.size());
  }

  @Test
  public void testJsonSerialization() throws Exception
  {
    CompactionStatusDetailedStats stats = new CompactionStatusDetailedStats();

    CompactionStatistics compactionStats = CompactionStatistics.create(100L, 10L, 1, 1);
    CompactionStatistics uncompactedStats = CompactionStatistics.create(100L, 10L, 1, 1);
    CompactionCandidate candidate = CompactionCandidate.from(
        List.of(SEGMENT_1),
        null,
        CompactionStatus.pending(compactionStats, uncompactedStats, List.of(SEGMENT_1), "Test")
    );
    stats.recordCompactionStatus(candidate, null);

    String json = objectMapper.writeValueAsString(stats);
    CompactionStatusDetailedStats deserialized = objectMapper.readValue(
        json,
        CompactionStatusDetailedStats.class
    );

    Map<CompactionStatus.State, Table> result = deserialized.getCompactionStates();
    Assert.assertEquals(1, result.size());
    Assert.assertTrue(result.containsKey(CompactionStatus.State.PENDING));
  }
}

