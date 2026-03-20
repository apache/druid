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

import org.apache.druid.data.input.MaxSizeSplitHintSpec;
import org.apache.druid.data.input.SegmentsSplitHintSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexer.partitions.SingleDimensionPartitionsSpec;
import org.apache.druid.indexer.report.IngestionStatsAndErrors;
import org.apache.druid.indexer.report.IngestionStatsAndErrorsTaskReport;
import org.apache.druid.indexer.report.TaskReport;
import org.apache.druid.indexing.common.task.TaskBuilder;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Optional;

/**
 * Runs tasks of "index_parallel" type using Indexers.
 */
public class IndexParallelTaskTest extends EmbeddedClusterTestBase
{
  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer()
      .addProperty("druid.worker.capacity", "20")
      .addProperty("druid.segment.handoff.pollDuration", "PT0.1s")
      .addProperty("druid.indexer.task.ignoreTimestampSpecForDruidInputSource", "true")
      .addProperty("druid.processing.intermediaryData.storage.type", "deepstore");
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedHistorical historical = new EmbeddedHistorical();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .useLatchableEmitter()
                               .addServer(overlord)
                               .addServer(coordinator)
                               .addServer(indexer)
                               .addServer(historical)
                               .addServer(broker)
                               .addServer(new EmbeddedRouter());
  }

  public static List<PartitionsSpec> getTestParamPartitionsSpec()
  {
    return List.of(
        new DynamicPartitionsSpec(null, null),
        new HashedPartitionsSpec(null, 2, null, null),
        new SingleDimensionPartitionsSpec(2, null, "namespace", false)
    );
  }

  @CsvSource({"true, 5000", "false, 1", "false, 0"})
  @ParameterizedTest(name = "isAvailabilityConfirmed={0}, availabilityTimeout={1}")
  public void test_segmentAvailabilityIsConfirmed_whenTaskWaits5secondsForHandoff(
      final boolean isSegmentAvailabilityConfirmed,
      final long segmentAvailabilityTimeoutMillis
  )
  {
    final TaskBuilder.IndexParallel indexTask =
        TaskBuilder.ofTypeIndexParallel()
                   .dataSource(dataSource)
                   .timestampColumn("timestamp")
                   .jsonInputFormat()
                   .localInputSourceWithFiles(Resources.DataFile.tinyWiki1Json())
                   .dimensions()
                   .tuningConfig(
                       t -> t.withAwaitSegmentAvailabilityTimeoutMillis(segmentAvailabilityTimeoutMillis)
                   );

    final String taskId = runTask(indexTask, dataSource);

    // Get the task report to verify that segment availability has been confirmed
    final TaskReport.ReportMap taskReport = cluster.callApi().onLeaderOverlord(
        o -> o.taskReportAsMap(taskId)
    );
    final Optional<IngestionStatsAndErrorsTaskReport> statsReportOptional
        = taskReport.findReport("ingestionStatsAndErrors");
    Assertions.assertTrue(statsReportOptional.isPresent());

    final IngestionStatsAndErrors statsAndErrors = statsReportOptional.get().getPayload();
    Assertions.assertEquals(
        isSegmentAvailabilityConfirmed,
        statsAndErrors.isSegmentAvailabilityConfirmed()
    );
  }

  @MethodSource("getTestParamPartitionsSpec")
  @ParameterizedTest(name = "partitionsSpec={0}")
  public void test_runIndexTask_andReindexIntoAnotherDatasource(PartitionsSpec partitionsSpec)
  {
    final boolean isRollup = partitionsSpec.isForceGuaranteedRollupCompatible();

    final TaskBuilder.IndexParallel indexTask = buildIndexParallelTask(partitionsSpec, false);

    runTask(indexTask, dataSource);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);
    runQueries(dataSource);

    // Re-index into a different datasource, indexing 1 segment per sub-task
    final String dataSource2 = EmbeddedClusterApis.createTestDatasourceName();
    final TaskBuilder.IndexParallel reindexTaskSplitBySegment =
        TaskBuilder.ofTypeIndexParallel()
                   .dataSource(dataSource2)
                   .isoTimestampColumn("ignored")
                   .druidInputSource(dataSource, Intervals.ETERNITY)
                   .tuningConfig(
                       t -> t.withPartitionsSpec(partitionsSpec)
                             .withMaxNumConcurrentSubTasks(10)
                             .withForceGuaranteedRollup(isRollup)
                             .withSplitHintSpec(new SegmentsSplitHintSpec(HumanReadableBytes.valueOf(1), null))
                   )
                   .dataSchema(
                       d -> d.withDimensions(
                           DimensionsSpec
                               .builder()
                               .setDimensionExclusions(List.of("robot", "continent"))
                               .build()
                       )
                   );

    runTask(reindexTaskSplitBySegment, dataSource2);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource2, coordinator, broker);
    runQueries(dataSource2);

    // Re-index into a different datasource, indexing 1 file per sub-task
    final String dataSource3 = EmbeddedClusterApis.createTestDatasourceName();
    final TaskBuilder.IndexParallel reindexTaskSplitByFile =
        TaskBuilder.ofTypeIndexParallel()
                   .dataSource(dataSource3)
                   .timestampColumn("timestamp")
                   .druidInputSource(dataSource, Intervals.ETERNITY)
                   .dataSchema(
                       d -> d.withDimensions(
                           DimensionsSpec
                               .builder()
                               .setDimensionExclusions(List.of("robot", "continent"))
                               .build()
                       )
                   )
                   .tuningConfig(
                       t -> t.withPartitionsSpec(partitionsSpec)
                             .withMaxNumConcurrentSubTasks(10)
                             .withForceGuaranteedRollup(isRollup)
                             .withSplitHintSpec(new MaxSizeSplitHintSpec(null, 1))
                   );

    runTask(reindexTaskSplitByFile, dataSource3);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource3, coordinator, broker);
    runQueries(dataSource3);
  }

  @MethodSource("getTestParamPartitionsSpec")
  @ParameterizedTest(name = "partitionsSpec={0}")
  public void test_runIndexTask_andAppendData(PartitionsSpec partitionsSpec)
  {
    final TaskBuilder.IndexParallel initialTask = buildIndexParallelTask(partitionsSpec, false);
    runTask(initialTask, dataSource);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);
    cluster.callApi().verifySqlQuery("SELECT COUNT(*) FROM %s", dataSource, "10");
    runGroupByQuery("Crimson Typhoon,1,905.0,9050.0");

    final TaskBuilder.IndexParallel appendTask
        = buildIndexParallelTask(new DynamicPartitionsSpec(null, null), true);
    runTask(appendTask, dataSource);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);
    cluster.callApi().verifySqlQuery("SELECT COUNT(*) FROM %s", dataSource, "20");
    runGroupByQuery("Crimson Typhoon,2,1810.0,18100.0");
  }

  /**
   * Creates a builder for an "index_parallel" task to ingest into {@link #dataSource}.
   */
  private TaskBuilder.IndexParallel buildIndexParallelTask(
      PartitionsSpec partitionsSpec,
      boolean appendToExisting
  )
  {
    final boolean isRollup = partitionsSpec.isForceGuaranteedRollupCompatible();

    return TaskBuilder.ofTypeIndexParallel()
                      .dataSource(dataSource)
                      .timestampColumn("timestamp")
                      .jsonInputFormat()
                      .localInputSourceWithFiles(
                          Resources.DataFile.tinyWiki1Json(),
                          Resources.DataFile.tinyWiki2Json(),
                          Resources.DataFile.tinyWiki3Json()
                      )
                      .segmentGranularity("DAY")
                      .dimensions("namespace", "page", "language")
                      .metricAggregates(
                          new DoubleSumAggregatorFactory("added", "added"),
                          new DoubleSumAggregatorFactory("deleted", "deleted"),
                          new DoubleSumAggregatorFactory("delta", "delta"),
                          new CountAggregatorFactory("count")
                      )
                      .appendToExisting(appendToExisting)
                      .tuningConfig(
                          t -> t.withPartitionsSpec(partitionsSpec)
                                .withForceGuaranteedRollup(isRollup)
                                .withMaxNumConcurrentSubTasks(10)
                                .withSplitHintSpec(new MaxSizeSplitHintSpec(1, null))
                      );
  }

  private String runTask(TaskBuilder.IndexParallel taskBuilder, String dataSource)
  {
    final String taskId = EmbeddedClusterApis.newTaskId(dataSource);
    cluster.callApi().runTask(taskBuilder.withId(taskId), overlord);
    return taskId;
  }

  private void runQueries(String dataSource)
  {
    Assertions.assertEquals(
        "10,2013-09-01T12:41:27.000Z,2013-08-31T01:02:33.000Z",
        cluster.runSql("SELECT COUNT(*), MAX(__time), MIN(__time) FROM %s", dataSource)
    );
    runGroupByQuery("Crimson Typhoon,1,905.0,9050.0");
  }

  private void runGroupByQuery(String expectedResult)
  {
    Assertions.assertEquals(
        expectedResult,
        cluster.runSql(
            "SELECT \"page\", COUNT(*) AS \"rows\", SUM(\"added\"), 10 * SUM(\"added\") AS added_times_ten"
            + " FROM %s"
            + " WHERE \"language\" = 'zh' AND __time < '2013-09-01'"
            + " GROUP BY 1"
            + " HAVING added_times_ten > 9000",
            dataSource
        )
    );
  }
}
