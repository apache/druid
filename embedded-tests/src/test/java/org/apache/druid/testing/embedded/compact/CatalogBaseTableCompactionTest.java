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

import org.apache.druid.catalog.guice.CatalogClientModule;
import org.apache.druid.catalog.guice.CatalogCoordinatorModule;
import org.apache.druid.catalog.model.ClusteredValueGroupsBaseTableMetadata;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.table.ClusterKeySpec;
import org.apache.druid.catalog.model.table.DatasourceDefn;
import org.apache.druid.catalog.model.table.TableBuilder;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.data.input.impl.ClusteredValueGroupsBaseTableProjectionSpec;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.indexer.granularity.SegmentGranularitySpec;
import org.apache.druid.indexer.partitions.DimensionRangePartitionsSpec;
import org.apache.druid.indexing.common.task.TaskBuilder;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexSupervisorTask;
import org.apache.druid.indexing.compact.CompactionSupervisorSpec;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.rpc.UpdateResponse;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.server.coordinator.CatalogDataSourceCompactionConfig;
import org.apache.druid.server.coordinator.ClusterCompactionConfig;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.catalog.TestCatalogClient;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.apache.druid.timeline.CompactionState;
import org.apache.druid.timeline.DataSegment;
import org.hamcrest.Matchers;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * End-to-end coverage of catalog-driven compaction of a clustered base table: the catalog's {@code baseTable} layout
 * drives the compacted segment's physical shape, and a table that is not {@code sealed} makes compaction analyze the
 * segments it is rewriting so columns the catalog does not declare survive.
 */
public class CatalogBaseTableCompactionTest extends EmbeddedClusterTestBase
{
  /**
   * Declared by the catalog table, so the base table spec owns it.
   */
  private static final String DECLARED_COLUMN = "channel";
  /**
   * Present in the segments but absent from the catalog table, so only an analyze step can find it.
   */
  private static final String UNDECLARED_COLUMN = "countryName";

  private final EmbeddedOverlord overlord = new EmbeddedOverlord()
      .addProperty("druid.catalog.client.maxSyncRetries", "0")
      .addProperty("druid.manager.segments.pollDuration", "PT1s")
      .addProperty("druid.manager.segments.useIncrementalCache", "always");
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator()
      .addProperty("druid.manager.segments.useIncrementalCache", "always");
  private final EmbeddedBroker broker = new EmbeddedBroker()
      .addProperty("druid.catalog.client.pollingPeriod", "100");
  // MSQ sizes its memory budget per worker slot, so the indexer needs headroom to run the compaction controller.
  private final EmbeddedIndexer indexer = new EmbeddedIndexer()
      .setServerMemory(2_000_000_000L)
      .addProperty("druid.worker.capacity", "4");

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster
        .withEmbeddedDerbyAndZookeeper()
        .useLatchableEmitter()
        .useDefaultTimeoutForLatchableEmitter(120)
        .addExtensions(CatalogClientModule.class, CatalogCoordinatorModule.class)
        // Clustered base-table segments require the V10 segment format.
        .addCommonProperty("druid.indexer.task.buildV10", "true")
        .addServer(coordinator)
        .addServer(overlord)
        .addServer(indexer)
        .addServer(broker)
        .addServer(new EmbeddedHistorical());
  }

  @Test
  public void test_compactClusteredBaseTable_withUnsealedCatalogTable_preservesUndeclaredColumns() throws IOException
  {
    ingestClusteredSegment();

    // The catalog declares only two of the three stored columns and says nothing about being sealed, so it accepts
    // columns it does not declare -- exactly the case where compaction has to go looking.
    createCatalogTable(false);
    configureCompaction();
    runCompaction();

    final CompactionState compactionState = Assertions.assertDoesNotThrow(
        () -> getOnlyUsedSegment().getLastCompactionState()
    );
    Assertions.assertNotNull(compactionState);
    Assertions.assertNotNull(compactionState.getBaseTable());

    final List<String> columns = compactionState.getBaseTable()
                                                .getDimensionsSpec()
                                                .getDimensions()
                                                .stream()
                                                .map(DimensionSchema::getName)
                                                .collect(Collectors.toList());
    // The declared shape leads, and the column the catalog knows nothing about was carried over rather than dropped.
    Assertions.assertEquals(
        List.of(DECLARED_COLUMN, ColumnHolder.TIME_COLUMN_NAME, UNDECLARED_COLUMN),
        columns
    );
    Assertions.assertEquals(
        List.of(DECLARED_COLUMN),
        ((ClusteredValueGroupsBaseTableProjectionSpec) compactionState.getBaseTable()).getClusteringColumnNames()
    );
  }

  @Test
  public void test_compactClusteredBaseTable_withSealedCatalogTable_dropsUndeclaredColumns() throws IOException
  {
    ingestClusteredSegment();

    // A sealed table declares everything, so the spec is taken as authoritative and the undeclared column is not
    // carried over. This is the counterpart of the test above: the same segments, the same layout, one property apart.
    createCatalogTable(true);
    configureCompaction();
    runCompaction();

    final CompactionState compactionState = getOnlyUsedSegment().getLastCompactionState();
    Assertions.assertNotNull(compactionState);
    Assertions.assertEquals(
        List.of(DECLARED_COLUMN, ColumnHolder.TIME_COLUMN_NAME),
        compactionState.getBaseTable()
                       .getDimensionsSpec()
                       .getDimensions()
                       .stream()
                       .map(DimensionSchema::getName)
                       .collect(Collectors.toList())
    );
  }

  @Test
  public void test_compactClusteredBaseTable_withClusterKeys_partitionsByRange() throws IOException
  {
    ingestClusteredSegment();

    // Cluster keys and a base table layout are different axes: the keys range-partition rows across segments, the
    // base table groups them into cluster groups within a segment. A table that declares both gets both.
    createCatalogTable(false, DECLARED_COLUMN);
    configureCompaction();
    runCompaction();

    final CompactionState compactionState = getOnlyUsedSegment().getLastCompactionState();
    Assertions.assertNotNull(compactionState);
    Assertions.assertInstanceOf(DimensionRangePartitionsSpec.class, compactionState.getPartitionsSpec());
    Assertions.assertEquals(
        List.of(DECLARED_COLUMN),
        ((DimensionRangePartitionsSpec) compactionState.getPartitionsSpec()).getPartitionDimensions()
    );
    // targetSegmentRows is used verbatim, not scaled by 1.5x, matching how an INSERT into this table sizes segments.
    Assertions.assertEquals(1000, compactionState.getPartitionsSpec().getMaxRowsPerSegment());
    // The base table layout still applies, undeclared column and all.
    Assertions.assertEquals(
        List.of(DECLARED_COLUMN, ColumnHolder.TIME_COLUMN_NAME, UNDECLARED_COLUMN),
        compactionState.getBaseTable()
                       .getDimensionsSpec()
                       .getDimensions()
                       .stream()
                       .map(DimensionSchema::getName)
                       .collect(Collectors.toList())
    );
  }

  /**
   * Posts the catalog compaction supervisor and waits until the coordinator reports nothing left awaiting compaction
   * for this datasource.
   * <p>
   * That wait is also the convergence assertion: the coordinator recomputes {@code interval/waitCompact/count} from
   * the recorded {@link CompactionState} against the config on every run, so if the appended columns made the
   * compacted segment look permanently out of date, the metric would never reach zero and this would time out rather
   * than pass.
   */
  private void runCompaction()
  {
    cluster.callApi().postSupervisor(
        new CompactionSupervisorSpec(
            new CatalogDataSourceCompactionConfig(
                dataSource,
                // baseTable requires the MSQ compaction engine
                CompactionEngine.MSQ,
                Period.ZERO,
                null,
                null,
                null,
                null,
                null
            ),
            false,
            null
        )
    );

    overlord.latchableEmitter().waitForEvent(
        event -> event.hasMetricName("task/run/time")
                      .hasDimension(DruidMetrics.TASK_TYPE, "compact")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource)
    );
    overlord.latchableEmitter().waitForEvent(
        event -> event.hasMetricName("interval/waitCompact/count")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource)
                      .hasValueMatching(Matchers.equalTo(0L))
    );
  }

  private void createCatalogTable(boolean sealed, String... clusterKeyColumns)
  {
    final TableBuilder builder =
        TableBuilder.datasource(dataSource, "HOUR")
                    .column(DECLARED_COLUMN, Columns.SQL_VARCHAR)
                    .column(Columns.TIME_COLUMN, Columns.LONG)
                    .baseTable(new ClusteredValueGroupsBaseTableMetadata(List.of(DECLARED_COLUMN), null, null));
    if (sealed) {
      builder.property(DatasourceDefn.SEALED_PROPERTY, true);
    }
    if (clusterKeyColumns.length > 0) {
      // Size the segments too, so the recorded range spec is the sized one: if the config and what MSQ records
      // disagreed on how targetSegmentRows maps onto a range spec, the interval would never read as compacted and
      // runCompaction() below would spin rather than settle.
      builder.property(DatasourceDefn.TARGET_SEGMENT_ROWS_PROPERTY, 1000);
      builder.clusterColumns(
          Arrays.stream(clusterKeyColumns)
                .map(column -> new ClusterKeySpec(column, false))
                .toArray(ClusterKeySpec[]::new)
      );
    }
    final TableMetadata table = builder.build();
    new TestCatalogClient(cluster).createTable(table, true);
  }

  private void configureCompaction()
  {
    final UpdateResponse updateResponse = cluster.callApi().onLeaderOverlord(
        o -> o.updateClusterCompactionConfig(
            new ClusterCompactionConfig(1.0, 10, null, true, CompactionEngine.MSQ, true)
        )
    );
    Assertions.assertTrue(updateResponse.isSuccess());
  }

  /**
   * Ingests a clustered V10 segment carrying {@link #UNDECLARED_COLUMN} in addition to what the catalog table below
   * declares, so compaction has a column to discover.
   */
  private void ingestClusteredSegment() throws IOException
  {
    final File inputFile = new File(cluster.getTestFolder().newFolder(), "clustered-input.json");
    Files.write(
        inputFile.toPath(),
        ("{\"time\":\"2024-01-01T00:10:00Z\",\"channel\":\"#en\",\"countryName\":\"US\"}\n"
         + "{\"time\":\"2024-01-01T00:20:00Z\",\"channel\":\"#en\",\"countryName\":\"CA\"}\n"
         + "{\"time\":\"2024-01-01T00:30:00Z\",\"channel\":\"#fr\",\"countryName\":\"FR\"}\n")
            .getBytes(StandardCharsets.UTF_8)
    );

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
                    new SegmentGranularitySpec(Granularities.HOUR, List.of(Intervals.of("2024-01-01/2024-01-02")))
                )
                .withBaseTable(
                    ClusteredValueGroupsBaseTableProjectionSpec
                        .builder()
                        .columns(
                            new StringDimensionSchema(DECLARED_COLUMN),
                            new LongDimensionSchema(ColumnHolder.TIME_COLUMN_NAME),
                            new StringDimensionSchema(UNDECLARED_COLUMN)
                        )
                        .clusteringColumns(DECLARED_COLUMN)
                        .build()
                )
        )
        .tuningConfig(t -> t.withMaxNumConcurrentSubTasks(1))
        .withId(taskId);

    cluster.callApi().onLeaderOverlord(o -> o.runTask(taskId, task));
    cluster.callApi().waitForTaskToSucceed(taskId, overlord);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);
  }

  private DataSegment getOnlyUsedSegment()
  {
    final List<DataSegment> segments = overlord
        .bindings()
        .segmentsMetadataStorage()
        .retrieveAllUsedSegments(dataSource, Segments.ONLY_VISIBLE)
        .stream()
        .filter(segment -> !segment.isTombstone())
        .collect(Collectors.toList());
    Assertions.assertEquals(1, segments.size(), segments::toString);
    return segments.get(0);
  }
}
