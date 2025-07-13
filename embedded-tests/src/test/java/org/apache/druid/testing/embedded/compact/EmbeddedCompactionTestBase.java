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

package org.apache.druid.testing.embedded.compact;

import org.apache.druid.indexing.common.task.TaskBuilder;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;
import org.junit.jupiter.api.Assertions;

import java.io.Closeable;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public abstract class EmbeddedCompactionTestBase extends EmbeddedClusterTestBase
{
  protected final EmbeddedOverlord overlord = new EmbeddedOverlord();
  protected final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .useLatchableEmitter()
                               .addServer(overlord)
                               .addServer(coordinator)
                               .addServer(new EmbeddedIndexer())
                               .addServer(new EmbeddedBroker())
                               .addServer(new EmbeddedHistorical())
                               .addServer(new EmbeddedRouter());
  }

  /**
   * Deletes all the data for the given datasource so that compaction tasks for
   * this datasource do not take up task slots unnecessarily.
   */
  protected Closeable unloader(String dataSource)
  {
    return () -> {
      overlord.bindings().segmentsMetadataStorage().markAllSegmentsAsUnused(dataSource);
    };
  }

  /**
   * Creates a Task using the given builder and runs it.
   *
   * @return ID of the task.
   */
  protected String runTask(TaskBuilder<?, ?, ?> taskBuilder, String dataSource)
  {
    final String taskId = EmbeddedClusterApis.newTaskId(dataSource);
    cluster.callApi().onLeaderOverlord(
        o -> o.runTask(taskId, taskBuilder.dataSource(dataSource).withId(taskId))
    );
    cluster.callApi().waitForTaskToSucceed(taskId, overlord);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator);

    return taskId;
  }

  protected void checkCompactionIntervals(List<String> expectedIntervals)
  {
    Assertions.assertEquals(
        Set.copyOf(expectedIntervals),
        Set.copyOf(getSegmentIntervals(dataSource))
    );
  }

  protected Set<DataSegment> getFullSegmentsMetadata(String dataSource)
  {
    return overlord
        .bindings()
        .segmentsMetadataStorage()
        .retrieveAllUsedSegments(dataSource, Segments.ONLY_VISIBLE);
  }

  protected List<String> getSegmentIntervals(String dataSource)
  {
    final Comparator<Interval> comparator = Comparators.intervalsByStartThenEnd().reversed();
    final Set<Interval> sortedIntervals = new TreeSet<>(comparator);

    final Set<DataSegment> allUsedSegments = getFullSegmentsMetadata(dataSource);
    for (DataSegment segment : allUsedSegments) {
      sortedIntervals.add(segment.getInterval());
    }

    return sortedIntervals.stream().map(Interval::toString).collect(Collectors.toList());
  }

  protected void verifySegmentsCount(int numExpectedSegments)
  {
    int segmentCount = getFullSegmentsMetadata(dataSource).size();
    Assertions.assertEquals(numExpectedSegments, segmentCount, "Segment count mismatch");
    Assertions.assertEquals(
        String.valueOf(segmentCount),
        cluster.runSql(
            "SELECT COUNT(*) FROM sys.segments"
            + " WHERE datasource='%s' AND is_overshadowed = 0 AND is_available = 1",
            dataSource
        ),
        "Segment count mismatch in sys table"
    );
  }
}
