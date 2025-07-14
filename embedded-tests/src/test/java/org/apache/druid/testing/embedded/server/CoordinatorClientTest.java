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

package org.apache.druid.testing.embedded.server;

import org.apache.druid.client.ImmutableSegmentLoadInfo;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.TaskBuilder;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.server.coordinator.rules.ForeverBroadcastDistributionRule;
import org.apache.druid.server.coordinator.rules.Rule;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.indexing.Resources;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentStatusInCluster;
import org.joda.time.Interval;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CoordinatorClientTest extends EmbeddedClusterTestBase
{
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedHistorical historical = new EmbeddedHistorical();


  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    indexer.addProperty("druid.segment.handoff.pollDuration", "PT0.1s");

    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .useLatchableEmitter()
                               .addServer(overlord)
                               .addServer(coordinator)
                               .addServer(indexer)
                               .addServer(historical)
                               .addServer(broker);
  }

  @Test
  public void test_findCurrentLeader()
  {
    URI currentLeader = cluster.callApi().onLeaderCoordinator(CoordinatorClient::findCurrentLeader);
    Assertions.assertEquals(8081, currentLeader.getPort());
  }

  @Test
  @Timeout(20)
  public void test_isHandoffComplete()
  {
    runIndexTask();
    final List<DataSegment> segments = new ArrayList<>(
        overlord.bindings().segmentsMetadataStorage().retrieveAllUsedSegments(dataSource, null)
    );
    DataSegment firstSegment = segments.get(0);
    Boolean result = cluster.callApi().onLeaderCoordinator(
        c -> c.isHandoffComplete(
            dataSource,
            new SegmentDescriptor(firstSegment.getInterval(), firstSegment.getVersion(), 0)
        )
    );
    Assertions.assertTrue(result);
  }

  @Test
  @Timeout(20)
  public void test_fetchSegment()
  {
    runIndexTask();
    final List<DataSegment> segments = new ArrayList<>(
        overlord.bindings().segmentsMetadataStorage().retrieveAllUsedSegments(dataSource, null)
    );
    DataSegment firstSegment = segments.get(0);
    DataSegment result = cluster.callApi().onLeaderCoordinator(
        c -> c.fetchSegment(
            dataSource,
            firstSegment.getId().toString(),
            true
        )
    );
    Assertions.assertEquals(firstSegment, result);
  }

  @Test
  @Timeout(20)
  public void test_fetchServerViewSegments()
  {
    runIndexTask();

    final List<DataSegment> segments = new ArrayList<>(
        overlord.bindings().segmentsMetadataStorage().retrieveAllUsedSegments(dataSource, null)
    );
    List<Interval> intervals = List.of(segments.get(0).getInterval());
    Iterable<ImmutableSegmentLoadInfo> segmentLoadInfo = cluster.callApi().onLeaderCoordinatorSync(
        c -> c.fetchServerViewSegments(dataSource, intervals));

    Assertions.assertTrue(segmentLoadInfo.iterator().hasNext());
    ImmutableSegmentLoadInfo segmentLoad = segmentLoadInfo.iterator().next();
    Assertions.assertEquals(segments.get(0), segmentLoad.getSegment());
  }

  @Test
  @Timeout(20)
  public void test_fetchUsedSegments()
  {
    runIndexTask();

    final List<DataSegment> segments = new ArrayList<>(
        overlord.bindings().segmentsMetadataStorage().retrieveAllUsedSegments(dataSource, null)
    );
    List<DataSegment> result = cluster.callApi().onLeaderCoordinator(
        c -> c.fetchUsedSegments(dataSource, List.of(Intervals.ETERNITY))
    );

    Assertions.assertEquals(segments.size(), result.size());
  }

  @Test
  @Timeout(20)
  public void test_fetchAllUsedSegmentsWithOvershadowedStatus() throws IOException
  {
    runIndexTask();

    try (CloseableIterator<SegmentStatusInCluster> iterator = cluster.callApi().onLeaderCoordinator(
        c -> c.fetchAllUsedSegmentsWithOvershadowedStatus(Set.of(dataSource), true))
    ) {
      Assertions.assertTrue(iterator.hasNext());
      SegmentStatusInCluster segmentStatus = iterator.next();
      Assertions.assertEquals(dataSource, segmentStatus.getDataSegment().getDataSource());
    }
  }

  @Test
  @Timeout(20)
  public void test_loadRules()
  {
    Rule broadcastRule = new ForeverBroadcastDistributionRule();
    cluster.callApi().onLeaderCoordinator(
        c -> c.updateRulesForDatasource(dataSource, List.of(broadcastRule))
    );
    Map<String, List<Rule>> rules = cluster.callApi().onLeaderCoordinator(CoordinatorClient::getRulesForAllDatasources);
    Assertions.assertFalse(rules.isEmpty());
    Assertions.assertEquals(List.of(broadcastRule), rules.get(dataSource));
  }

  private void runIndexTask()
  {
    final String taskId = IdUtils.getRandomId();
    final Task task = TaskBuilder.ofTypeIndex()
                                 .dataSource(dataSource)
                                 .isoTimestampColumn("time")
                                 .csvInputFormatWithColumns("time", "item", "value")
                                 .inlineInputSourceWithData(Resources.InlineData.CSV_10_DAYS)
                                 .segmentGranularity("DAY")
                                 .dimensions()
                                 .withId(taskId);

    cluster.callApi().onLeaderOverlord(o -> o.runTask(taskId, task));
    cluster.callApi().waitForTaskToSucceed(taskId, overlord);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator);
  }
}
