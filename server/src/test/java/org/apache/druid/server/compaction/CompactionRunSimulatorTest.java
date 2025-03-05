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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.client.indexing.TaskPayloadResponse;
import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.metadata.LockFilterPolicy;
import org.apache.druid.rpc.indexing.NoopOverlordClient;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.DruidCompactionConfig;
import org.apache.druid.server.coordinator.simulate.TestSegmentsMetadataManager;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CompactionRunSimulatorTest
{
  private static final ObjectMapper OBJECT_MAPPER = new DefaultObjectMapper();

  private final CompactionRunSimulator simulator = new CompactionRunSimulator(
      new CompactionStatusTracker(OBJECT_MAPPER),
      new TestOverlordClient()
  );

  @Test
  public void testSimulateClusterCompactionConfigUpdate()
  {
    final TestSegmentsMetadataManager segmentsMetadataManager = new TestSegmentsMetadataManager();

    // Add some segments to the timeline
    final List<DataSegment> wikiSegments
        = CreateDataSegments.ofDatasource("wiki")
                            .forIntervals(10, Granularities.DAY)
                            .withNumPartitions(10)
                            .startingAt("2013-01-01")
                            .eachOfSizeInMb(100);
    wikiSegments.forEach(segmentsMetadataManager::addSegment);

    final CompactionSimulateResult simulateResult = simulator.simulateRunWithConfig(
        DruidCompactionConfig.empty().withDatasourceConfig(
            DataSourceCompactionConfig.builder().forDataSource("wiki").build()
        ),
        segmentsMetadataManager.getSnapshotOfDataSourcesWithAllUsedSegments(),
        CompactionEngine.NATIVE
    );

    Assert.assertNotNull(simulateResult);

    final Map<CompactionStatus.State, Table> compactionStates = simulateResult.getCompactionStates();
    Assert.assertNotNull(compactionStates);

    Assert.assertNull(compactionStates.get(CompactionStatus.State.COMPLETE));
    Assert.assertNull(compactionStates.get(CompactionStatus.State.RUNNING));

    final Table queuedTable = compactionStates.get(CompactionStatus.State.PENDING);
    Assert.assertEquals(
        Arrays.asList("dataSource", "interval", "numSegments", "bytes", "maxTaskSlots", "reasonToCompact"),
        queuedTable.getColumnNames()
    );
    Assert.assertEquals(
        Arrays.asList(
            Arrays.asList("wiki", Intervals.of("2013-01-09/P1D"), 10, 1_000_000_000L, 1, "not compacted yet"),
            Arrays.asList("wiki", Intervals.of("2013-01-08/P1D"), 10, 1_000_000_000L, 1, "not compacted yet"),
            Arrays.asList("wiki", Intervals.of("2013-01-07/P1D"), 10, 1_000_000_000L, 1, "not compacted yet"),
            Arrays.asList("wiki", Intervals.of("2013-01-06/P1D"), 10, 1_000_000_000L, 1, "not compacted yet"),
            Arrays.asList("wiki", Intervals.of("2013-01-05/P1D"), 10, 1_000_000_000L, 1, "not compacted yet"),
            Arrays.asList("wiki", Intervals.of("2013-01-04/P1D"), 10, 1_000_000_000L, 1, "not compacted yet"),
            Arrays.asList("wiki", Intervals.of("2013-01-03/P1D"), 10, 1_000_000_000L, 1, "not compacted yet"),
            Arrays.asList("wiki", Intervals.of("2013-01-02/P1D"), 10, 1_000_000_000L, 1, "not compacted yet"),
            Arrays.asList("wiki", Intervals.of("2013-01-01/P1D"), 10, 1_000_000_000L, 1, "not compacted yet")
        ),
        queuedTable.getRows()
    );

    final Table skippedTable = compactionStates.get(CompactionStatus.State.SKIPPED);
    Assert.assertEquals(
        Arrays.asList("dataSource", "interval", "numSegments", "bytes", "reasonToSkip"),
        skippedTable.getColumnNames()
    );
    Assert.assertEquals(
        Collections.singletonList(
            Arrays.asList("wiki", Intervals.of("2013-01-10/P1D"), 10, 1_000_000_000L, "skip offset from latest[P1D]")
        ),
        skippedTable.getRows()
    );
  }

  private static class TestOverlordClient extends NoopOverlordClient
  {
    @Override
    public ListenableFuture<CloseableIterator<TaskStatusPlus>> taskStatuses(
        @Nullable String state,
        @Nullable String dataSource,
        @Nullable Integer maxCompletedTasks
    )
    {
      return Futures.immediateFuture(CloseableIterators.withEmptyBaggage(Collections.emptyIterator()));
    }

    @Override
    public ListenableFuture<Map<String, TaskStatus>> taskStatuses(Set<String> taskIds)
    {
      return Futures.immediateFuture(Collections.emptyMap());
    }

    @Override
    public ListenableFuture<TaskPayloadResponse> taskPayload(String taskId)
    {
      return Futures.immediateFuture(null);
    }

    @Override
    public ListenableFuture<Void> runTask(String taskId, Object taskObject)
    {
      return Futures.immediateVoidFuture();
    }

    @Override
    public ListenableFuture<Void> cancelTask(String taskId)
    {
      return Futures.immediateVoidFuture();
    }

    @Override
    public ListenableFuture<Map<String, List<Interval>>> findLockedIntervals(List<LockFilterPolicy> lockFilterPolicies)
    {
      return Futures.immediateFuture(Collections.emptyMap());
    }
  }
}
