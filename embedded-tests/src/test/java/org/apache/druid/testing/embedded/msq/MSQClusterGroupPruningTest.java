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

package org.apache.druid.testing.embedded.msq;

import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.data.input.impl.ClusteredValueGroupsBaseTableProjectionSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.granularity.SegmentGranularitySpec;
import org.apache.druid.indexing.common.task.TaskBuilder;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexSupervisorTask;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.apache.druid.timeline.ClusterGroupTuples;
import org.apache.druid.timeline.DataSegment;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Task-engine MSQ query test for e2e cluster group pruning. The segments under test are purposely using dynamic
 * partitioning so that the filtered segment pruner must use cluster group info to do pruning.
 */
public class MSQClusterGroupPruningTest extends EmbeddedClusterTestBase
{
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer()
      .setServerMemory(300_000_000L)
      .addProperty("druid.worker.capacity", "2");

  private EmbeddedMSQApis msqApis;

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster
        .withEmbeddedDerbyAndZookeeper()
        .useLatchableEmitter()
        // Clustered base-table segments can only be written in the V10 segment format.
        .addCommonProperty("druid.indexer.task.buildV10", "true")
        .addServer(overlord)
        .addServer(coordinator)
        .addServer(indexer)
        .addServer(broker)
        .addServer(new EmbeddedHistorical());
  }

  @BeforeAll
  void loadData() throws IOException
  {
    msqApis = new EmbeddedMSQApis(cluster, overlord);
    dataSource = "cluster-prune-" + IdUtils.getRandomId();

    // Three segments for the same day: two hold only tenant "acme", one holds only tenant "globex".
    ingest(false, rows("acme", "us", 3));
    ingest(true, rows("acme", "eu", 2));
    ingest(true, rows("globex", "us", 4));
  }

  @Override
  protected void refreshDatasourceName()
  {
    // Keep the datasource loaded in loadData() for every test.
  }

  @Test
  void testSegmentsHoldOneTenantEachWithoutRanges()
  {
    final Set<DataSegment> segments = cluster.callApi().getVisibleUsedSegments(dataSource, overlord);
    Assertions.assertEquals(3, segments.size());
    for (DataSegment segment : segments) {
      Assertions.assertTrue(segment.getShardSpec().getDomainDimensions().isEmpty());
      Assertions.assertEquals(1, tenantsOf(segment).size(), "segment[" + segment.getId() + "]");
    }
    Assertions.assertEquals(
        List.of("acme", "acme", "globex"),
        segments.stream().map(segment -> tenantsOf(segment).iterator().next()).sorted().toList()
    );
  }

  @Test
  void testUnfilteredQueryReadsEverySegment()
  {
    final MSQTaskReportPayload report = msqApis.runTaskSqlAndGetReport("SELECT COUNT(*) FROM \"%s\"", dataSource);

    Assertions.assertEquals(9L, onlyValue(report));
    final EmbeddedMSQApis.ChannelSums input = msqApis.getInputChannelSums(report, 0);
    Assertions.assertEquals(3, input.files());
    Assertions.assertEquals(9, input.rows());
  }

  @Test
  void testFilterOnClusteringColumnReadsOnlyMatchingSegments()
  {
    final MSQTaskReportPayload report = msqApis.runTaskSqlAndGetReport(
        "SELECT COUNT(*) FROM \"%s\" WHERE \"tenant\" <> 'acme'",
        dataSource
    );

    Assertions.assertEquals(4L, onlyValue(report));
    final EmbeddedMSQApis.ChannelSums input = msqApis.getInputChannelSums(report, 0);
    Assertions.assertEquals(1, input.files(), "only the tenant globex segment should be read");
    Assertions.assertEquals(4, input.rows());
  }

  private static Set<String> tenantsOf(DataSegment segment)
  {
    final ClusterGroupTuples groups = segment.getClusterGroups();
    Assertions.assertNotNull(groups, "segment[" + segment.getId() + "] should have cluster groups");
    return groups.tuples().stream().map(tuple -> String.valueOf(tuple.get(0))).collect(Collectors.toSet());
  }

  private static long onlyValue(MSQTaskReportPayload report)
  {
    final List<Object[]> results = report.getResults().getResults();
    Assertions.assertEquals(1, results.size());
    return ((Number) results.get(0)[0]).longValue();
  }

  private static String rows(String tenant, String region, int count)
  {
    final StringBuilder builder = new StringBuilder();
    for (int i = 0; i < count; i++) {
      builder.append(
          "{\"time\":\"2024-01-01T0" + i + ":00:00Z\",\"tenant\":\"" + tenant
          + "\",\"region\":\"" + region + "\",\"value\":" + (i + 1) + "}\n"
      );
    }
    return builder.toString();
  }

  private void ingest(boolean appendToExisting, String inputData) throws IOException
  {
    final File inputFile = new File(cluster.getTestFolder().newFolder(), "input.json");
    Files.write(inputFile.toPath(), inputData.getBytes(StandardCharsets.UTF_8));

    final ClusteredValueGroupsBaseTableProjectionSpec baseTable =
        ClusteredValueGroupsBaseTableProjectionSpec.builder()
            .columns(
                new StringDimensionSchema("tenant"),
                new StringDimensionSchema("region"),
                new LongDimensionSchema("value"),
                new LongDimensionSchema("__time")
            )
            .clusteringColumns("tenant")
            .build();

    final String taskId = IdUtils.getRandomId();
    final ParallelIndexSupervisorTask task = TaskBuilder
        .ofTypeIndexParallel()
        .jsonInputFormat()
        .localInputSourceWithFiles(inputFile)
        .dataSchema(
            builder -> builder
                .withDataSource(dataSource)
                .withTimestamp(new TimestampSpec("time", "iso", null))
                .withSegmentGranularity(
                    new SegmentGranularitySpec(Granularities.DAY, List.of(Intervals.of("2024-01-01/2024-01-02")))
                )
                .withBaseTable(baseTable)
        )
        .appendToExisting(appendToExisting)
        .tuningConfig(t -> t.withMaxNumConcurrentSubTasks(1))
        .withId(taskId);

    cluster.callApi().onLeaderOverlord(o -> o.runTask(taskId, task));
    cluster.callApi().waitForTaskToSucceed(taskId, overlord);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);
  }
}
