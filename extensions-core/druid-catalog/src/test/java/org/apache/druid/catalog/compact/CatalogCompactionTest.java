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

package org.apache.druid.catalog.compact;

import org.apache.druid.catalog.guice.CatalogClientModule;
import org.apache.druid.catalog.guice.CatalogCoordinatorModule;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.table.TableBuilder;
import org.apache.druid.catalog.sync.CatalogClient;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.indexing.common.task.TaskBuilder;
import org.apache.druid.indexing.compact.CompactionSupervisorSpec;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.rpc.UpdateResponse;
import org.apache.druid.server.coordinator.CatalogDataSourceCompactionConfig;
import org.apache.druid.server.coordinator.ClusterCompactionConfig;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class CatalogCompactionTest extends EmbeddedClusterTestBase
{
  private final EmbeddedOverlord overlord = new EmbeddedOverlord()
      .addProperty("druid.catalog.client.maxSyncRetries", "0");
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator()
      .addProperty("druid.manager.segments.useIncrementalCache", "always");
  private final EmbeddedBroker broker = new EmbeddedBroker()
      .addProperty("druid.catalog.client.pollingPeriod", "100");

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .useLatchableEmitter()
                               .addExtension(CatalogClientModule.class)
                               .addExtension(CatalogCoordinatorModule.class)
                               .addServer(overlord)
                               .addServer(coordinator)
                               .addServer(broker)
                               .addServer(new EmbeddedIndexer())
                               .addServer(new EmbeddedHistorical());
  }

  @Test
  public void test_ingestDayGranularity_andCompactToMonthGranularity()
  {
    // Ingest data at DAY granularity and verify
    runIngestionAtDayGranularity();
    List<DataSegment> segments = List.copyOf(
        overlord.bindings()
                .segmentsMetadataStorage()
                .retrieveAllUsedSegments(dataSource, Segments.ONLY_VISIBLE)
    );
    Assertions.assertEquals(3, segments.size());
    segments.forEach(
        segment -> Assertions.assertTrue(Granularities.DAY.isAligned(segment.getInterval()))
    );

    final CatalogClient catalogClient = broker.bindings().getInstance(CatalogClient.class);

    // Create a catalog entry to have MONTH granularity for the datasource
    final TableId tableId = new TableId("druid", dataSource);
    catalogClient.createTable(
        tableId,
        TableBuilder.datasource(dataSource, "P1M").buildSpec()
    );

    final TableMetadata metadata = catalogClient.table(tableId);
    Assertions.assertEquals(
        StringUtils.format("\"druid\".\"%s\"", dataSource),
        metadata.sqlName()
    );
    Assertions.assertEquals(
        TableBuilder.datasource(dataSource, "P1M").buildSpec(),
        metadata.spec()
    );

    enableCompactionSupervisor();

    // Create a catalog compaction config
    CatalogDataSourceCompactionConfig compactionConfig =
        new CatalogDataSourceCompactionConfig(dataSource, null, Period.ZERO, null, null, null, null);

    final CompactionSupervisorSpec compactionSupervisor
        = new CompactionSupervisorSpec(compactionConfig, false, null);
    cluster.callApi().postSupervisor(compactionSupervisor);

    // Wait for compaction to finish
    overlord.latchableEmitter().waitForEvent(
        event -> event.hasMetricName("task/run/time")
                      .hasDimension("taskType", "compact")
    );

    // Verify that segments are now compacted to MONTH granularity
    segments = List.copyOf(
        overlord.bindings()
                .segmentsMetadataStorage()
                .retrieveAllUsedSegments(dataSource, Segments.ONLY_VISIBLE)
    );
    Assertions.assertEquals(1, segments.size());
    Assertions.assertTrue(
        Granularities.MONTH.isAligned(segments.get(0).getInterval())
    );
  }

  private void runIngestionAtDayGranularity()
  {
    final String taskId = IdUtils.getRandomId();
    final IndexTask task = createIndexTaskForInlineData(taskId);

    cluster.callApi().runTask(task, overlord);
  }

  private IndexTask createIndexTaskForInlineData(String taskId)
  {
    final String inlineDataCsv =
        "2025-06-01T00:00:00.000Z,shirt,105"
        + "\n2025-06-02T00:00:00.000Z,trousers,210"
        + "\n2025-06-03T00:00:00.000Z,jeans,150";
    return TaskBuilder.ofTypeIndex()
                      .dataSource(dataSource)
                      .isoTimestampColumn("time")
                      .csvInputFormatWithColumns("time", "item", "value")
                      .inlineInputSourceWithData(inlineDataCsv)
                      .segmentGranularity("DAY")
                      .dimensions()
                      .withId(taskId);
  }

  private void enableCompactionSupervisor()
  {
    final UpdateResponse updateResponse = cluster.callApi().onLeaderOverlord(
        o -> o.updateClusterCompactionConfig(new ClusterCompactionConfig(1.0, 10, null, true, null))
    );
    Assertions.assertTrue(updateResponse.isSuccess());
  }
}
