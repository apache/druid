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

package org.apache.druid.testing.embedded.indexing;

import org.apache.druid.indexing.common.task.TaskBuilder;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class ConcurrentAppendReplaceTest extends EmbeddedClusterTestBase
{
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .useLatchableEmitter()
                               .addServer(overlord)
                               .addServer(coordinator)
                               .addServer(new EmbeddedIndexer())
                               .addServer(new EmbeddedBroker())
                               .addServer(new EmbeddedHistorical());
  }

  @Test
  public void test_concurrentAppend_toIntervalWithUnusedSegment_usesNewSegmentId()
  {
    // Run an APPEND task to ingest data into an interval
    final String data1Row = "2013-01-01T00:00:00.000Z,shirt,100";
    final String task1 = EmbeddedClusterApis.newTaskId(dataSource);
    final TaskBuilder.Index taskBuilder =
        TaskBuilder.ofTypeIndex()
                   .dataSource(dataSource)
                   .csvInputFormatWithColumns("time", "item", "value")
                   .isoTimestampColumn("time")
                   .inlineInputSourceWithData(data1Row)
                   .appendToExisting(true)
                   .context("useConcurrentLocks", true)
                   .dimensions();
    cluster.callApi().runTask(taskBuilder.withId(task1), overlord);

    List<DataSegment> usedSegments = getAllUsedSegments();
    Assertions.assertEquals(1, usedSegments.size());

    final SegmentId segmentId1 = usedSegments.get(0).getId();
    Assertions.assertEquals("1970-01-01T00:00:00.000Z", segmentId1.getVersion());
    Assertions.assertEquals(0, segmentId1.getPartitionNum());

    // Mark all segments as unused and verify that the interval is now empty
    overlord.bindings().segmentsMetadataStorage().markAllSegmentsAsUnused(dataSource);
    usedSegments = getAllUsedSegments();
    Assertions.assertTrue(usedSegments.isEmpty());

    // Run the APPEND task again with a different taskId
    final String task2 = EmbeddedClusterApis.newTaskId(dataSource);
    cluster.callApi().runTask(taskBuilder.withId(task2), overlord);

    // Verify that the new segment gets appended with the same version but a different ID
    usedSegments = getAllUsedSegments();
    Assertions.assertEquals(1, usedSegments.size());

    final SegmentId segmentId2 = usedSegments.get(0).getId();
    Assertions.assertEquals("1970-01-01T00:00:00.000ZS", segmentId2.getVersion());
    Assertions.assertEquals(0, segmentId2.getPartitionNum());

    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator);
    Assertions.assertEquals(
        data1Row,
        cluster.runSql("SELECT * FROM %s", dataSource)
    );
  }

  private List<DataSegment> getAllUsedSegments()
  {
    return List.copyOf(
        overlord.bindings()
                .segmentsMetadataStorage()
                .retrieveAllUsedSegments(dataSource, Segments.ONLY_VISIBLE)
    );
  }
}
