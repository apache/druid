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
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.server.coordinator.CoordinatorCompactionConfig;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.simulate.TestSegmentsMetadataManager;
import org.apache.druid.server.http.CompactionConfigUpdateRequest;
import org.apache.druid.timeline.DataSegment;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class CompactionRunSimulatorTest
{
  private static final ObjectMapper OBJECT_MAPPER = new DefaultObjectMapper();

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

    final CompactionSimulateResult simulateResult = new CompactionRunSimulator(
        new CompactionStatusTracker(OBJECT_MAPPER),
        CoordinatorCompactionConfig.from(
            Collections.singletonList(
                DataSourceCompactionConfig.builder().forDataSource("wiki").build()
            )
        ),
        segmentsMetadataManager.getSnapshotOfDataSourcesWithAllUsedSegments()
                               .getUsedSegmentsTimelinesPerDataSource()
    ).simulateRunWithConfigUpdate(
        new CompactionConfigUpdateRequest(null, null, null, null, null)
    );
    Assert.assertNotNull(simulateResult);

    Assert.assertEquals(
        Arrays.asList(
            Arrays.asList("dataSource", "interval", "numSegments", "bytes", "maxTaskSlots", "reasonToCompact"),
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
        simulateResult.getIntervalsToCompact()
    );
  }
}
