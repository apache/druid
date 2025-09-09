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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.druid.client.indexing.ClientCompactionTaskQuery;
import org.apache.druid.client.indexing.TaskPayloadResponse;
import org.apache.druid.data.input.MaxSizeSplitHintSpec;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexer.granularity.GranularitySpec;
import org.apache.druid.indexer.granularity.UniformGranularitySpec;
import org.apache.druid.indexer.partitions.DimensionRangePartitionsSpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexing.common.task.TaskBuilder;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FloatSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchBuildAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchModule;
import org.apache.druid.query.aggregation.datasketches.quantiles.DoublesSketchAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.quantiles.DoublesSketchModule;
import org.apache.druid.query.aggregation.datasketches.theta.SketchMergeAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.theta.SketchModule;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.AutoTypeColumnSchema;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.transform.CompactionTransformSpec;
import org.apache.druid.server.compaction.FixedIntervalOrderPolicy;
import org.apache.druid.server.coordinator.AutoCompactionSnapshot;
import org.apache.druid.server.coordinator.ClusterCompactionConfig;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.DruidCompactionConfig;
import org.apache.druid.server.coordinator.InlineSchemaDataSourceCompactionConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskDimensionsConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskGranularityConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskIOConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskQueryTuningConfig;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.indexing.MoreResources;
import org.apache.druid.testing.embedded.indexing.Resources;
import org.apache.druid.timeline.DataSegment;
import org.hamcrest.Matcher;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.joda.time.chrono.ISOChronology;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Embedded mode of integration-tests originally present in {@code ITAutoCompactionTest}.
 */
public class AutoCompactionTest extends CompactionTestBase
{
  private static final Logger LOG = new Logger(AutoCompactionTest.class);
  private static final Supplier<TaskBuilder.Index> INDEX_TASK = MoreResources.Task.INDEX_TASK_WITH_AGGREGATORS;

  private static final Supplier<TaskBuilder.Index> INDEX_TASK_WITH_GRANULARITY_SPEC =
      () -> INDEX_TASK.get().dimensions("language").dynamicPartitionWithMaxRows(10);
  private static final Supplier<TaskBuilder.Index> INDEX_TASK_WITH_DIMENSION_SPEC =
      () -> INDEX_TASK.get().granularitySpec("DAY", "DAY", true);

  private static final List<Pair<String, String>> INDEX_QUERIES_RESOURCE = List.of(
      Pair.of(Resources.Query.SELECT_MIN_MAX_TIME, "2013-08-31T01:02:33.000Z,2013-09-01T12:41:27.000Z"),
      Pair.of(Resources.Query.SELECT_APPROX_COUNT_DISTINCT, "5,5"),
      Pair.of(Resources.Query.SELECT_EARLIEST_LATEST_USER, "nuclear,stringer"),
      Pair.of(Resources.Query.SELECT_COUNT_OF_CHINESE_PAGES, "Crimson Typhoon,1,905.0,9050.0")
  );
  private static final Supplier<TaskBuilder.IndexParallel> INDEX_TASK_WITH_ROLLUP_FOR_PRESERVE_METRICS =
      () -> TaskBuilder
          .ofTypeIndexParallel()
          .jsonInputFormat()
          .inlineInputSourceWithData(Resources.InlineData.JSON_2_ROWS)
          .isoTimestampColumn("timestamp")
          .appendToExisting(true)
          .granularitySpec("DAY", "HOUR", true)
          .metricAggregates(
              new SketchMergeAggregatorFactory("thetaSketch", "user", null, null, null, null),
              new HllSketchBuildAggregatorFactory("HLLSketchBuild", "user", null, null, null, null, true),
              new DoublesSketchAggregatorFactory("quantilesDoublesSketch", "delta", null),
              new CountAggregatorFactory("ingested_events"),
              new LongSumAggregatorFactory("sum_added", "added"),
              new LongSumAggregatorFactory("sum_deleted", "deleted"),
              new LongSumAggregatorFactory("sum_delta", "delta"),
              new LongSumAggregatorFactory("sum_deltaBucket", "deltaBucket"),
              new LongSumAggregatorFactory("sum_commentLength", "commentLength")
          )
          .dimensions(
              "isRobot",
              "language", "flags", "isUnpatrolled", "page", "diffUrl", "comment",
              "isNew", "isMinor", "isAnonymous", "namespace"
          );

  private static final Supplier<TaskBuilder.IndexParallel> INDEX_TASK_WITHOUT_ROLLUP_FOR_PRESERVE_METRICS =
      () -> TaskBuilder
          .ofTypeIndexParallel()
          .jsonInputFormat()
          .inlineInputSourceWithData(Resources.InlineData.JSON_1_ROW)
          .isoTimestampColumn("timestamp")
          .granularitySpec("DAY", "HOUR", false)
          .appendToExisting(true)
          .dataSchema(
              d -> d.withDimensions(
                  new StringDimensionSchema("isRobot"),
                  new StringDimensionSchema("language"),
                  new StringDimensionSchema("flags"),
                  new StringDimensionSchema("isUnpatrolled"),
                  new StringDimensionSchema("page"),
                  new StringDimensionSchema("diffUrl"),
                  new LongDimensionSchema("added"),
                  new StringDimensionSchema("comment"),
                  new LongDimensionSchema("commentLength"),
                  new StringDimensionSchema("isNew"),
                  new StringDimensionSchema("isMinor"),
                  new LongDimensionSchema("delta"),
                  new StringDimensionSchema("isAnonymous"),
                  new StringDimensionSchema("user"),
                  new LongDimensionSchema("deltaBucket"),
                  new LongDimensionSchema("deleted"),
                  new StringDimensionSchema("namespace"),
                  new StringDimensionSchema("cityName"),
                  new StringDimensionSchema("countryName"),
                  new StringDimensionSchema("regionIsoCode"),
                  new StringDimensionSchema("metroCode"),
                  new StringDimensionSchema("countryIsoCode"),
                  new StringDimensionSchema("regionName")
              )
          );

  private static final int MAX_ROWS_PER_SEGMENT_COMPACTED = 10000;
  private static final Period NO_SKIP_OFFSET = Period.seconds(0);
  private static final FixedIntervalOrderPolicy COMPACT_NOTHING_POLICY = new FixedIntervalOrderPolicy(List.of());

  public static List<CompactionEngine> getEngine()
  {
    return List.of(CompactionEngine.NATIVE);
  }
  
  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .useLatchableEmitter()
                               .addExtension(SketchModule.class)
                               .addExtension(HllSketchModule.class)
                               .addExtension(DoublesSketchModule.class)
                               .addServer(overlord)
                               .addServer(coordinator)
                               .addServer(broker)
                               .addServer(new EmbeddedIndexer().addProperty("druid.worker.capacity", "10"))
                               .addServer(new EmbeddedHistorical())
                               .addServer(new EmbeddedRouter());
  }

  protected CompactionResourceTestClient compactionResource;

  private String fullDatasourceName;

  @BeforeAll
  public void setupClient()
  {
    this.compactionResource = new CompactionResourceTestClient(cluster);
  }

  @BeforeEach
  public void resetCompactionTaskSlots()
  {
    // Set compaction slot to 5
    updateCompactionTaskSlot(0.5, 10);
    fullDatasourceName = dataSource;
  }

  @Test
  public void testAutoCompactionRowWithMetricAndRowWithoutMetricShouldPreserveExistingMetricsUsingAggregatorWithDifferentReturnType() throws Exception
  {
    // added = null, count = 2, sum_added = 62, quantilesDoublesSketch = 2, thetaSketch = 2, HLLSketchBuild = 2
    loadData(INDEX_TASK_WITH_ROLLUP_FOR_PRESERVE_METRICS);
    // added = 31, count = null, sum_added = null, quantilesDoublesSketch = null, thetaSketch = null, HLLSketchBuild = null
    loadData(INDEX_TASK_WITHOUT_ROLLUP_FOR_PRESERVE_METRICS);
    try (final Closeable ignored = unloader(fullDatasourceName)) {
      final List<Interval> intervalsBeforeCompaction = getSegmentIntervals();

      // 2 segments across 1 days...
      verifySegmentsCount(2);
      verifyScanResult("added", "...\n31");
      verifyScanResult("ingested_events", "2\n...");
      verifyScanResult("sum_added", "62\n...");
      verifyScanResult("COUNT(*)", "2");
      verifyDistinctCount("2,2");

      submitCompactionConfig(
          MAX_ROWS_PER_SEGMENT_COMPACTED,
          NO_SKIP_OFFSET,
          new UserCompactionTaskGranularityConfig(null, null, true),
          new UserCompactionTaskDimensionsConfig(DimensionsSpec.getDefaultSchemas(ImmutableList.of("language"))),
          null,
          new AggregatorFactory[]{
              new CountAggregatorFactory("ingested_events"),
              // FloatSumAggregator combine method takes in two Float but return Double
              new FloatSumAggregatorFactory("sum_added", "added"),
              new SketchMergeAggregatorFactory("thetaSketch", "user", 16384, true, false, null),
              new HllSketchBuildAggregatorFactory("HLLSketchBuild", "user", 12, TgtHllType.HLL_4.name(), null, false, false),
              new DoublesSketchAggregatorFactory("quantilesDoublesSketch", "delta", 128, 1000000000L, null)
          },
          false,
          CompactionEngine.NATIVE
      );
      // should now only have 1 row after compaction
      // added = null, count = 3, sum_added = 93.0
      forceTriggerAutoCompaction(1);

      verifyScanResult("added", "...");
      verifyScanResult("ingested_events", "3");
      verifyScanResult("sum_added", "93.0");
      verifyScanResult("COUNT(*)", "1");
      verifyDistinctCount("3,3");

      verifySegmentsCompacted(1, MAX_ROWS_PER_SEGMENT_COMPACTED);
      verifySegmentIntervals(intervalsBeforeCompaction);

      List<TaskStatusPlus> compactTasksBefore = getCompleteTasksForDataSource(fullDatasourceName);
      // Verify rollup segments does not get compacted again
      forceTriggerAutoCompaction(1);
      List<TaskStatusPlus> compactTasksAfter = getCompleteTasksForDataSource(fullDatasourceName);
      Assertions.assertEquals(compactTasksAfter.size(), compactTasksBefore.size());
    }
  }

  @Test
  public void testAutoCompactionRowWithMetricAndRowWithoutMetricShouldPreserveExistingMetrics() throws Exception
  {
    // added = null, count = 2, sum_added = 62, quantilesDoublesSketch = 2, thetaSketch = 2, HLLSketchBuild = 2
    loadData(INDEX_TASK_WITH_ROLLUP_FOR_PRESERVE_METRICS);
    // added = 31, count = null, sum_added = null, quantilesDoublesSketch = null, thetaSketch = null, HLLSketchBuild = null
    loadData(INDEX_TASK_WITHOUT_ROLLUP_FOR_PRESERVE_METRICS);
    try (final Closeable ignored = unloader(fullDatasourceName)) {
      final List<Interval> intervalsBeforeCompaction = getSegmentIntervals();

      // 2 segments across 1 days...
      verifySegmentsCount(2);

      verifyScanResult("COUNT(*)", "2");
      verifyScanResult("added", "...\n31");
      verifyScanResult("ingested_events", "2\n...");
      verifyScanResult("sum_added", "62\n...");
      verifyDistinctCount("2,2");

      submitCompactionConfig(
          MAX_ROWS_PER_SEGMENT_COMPACTED,
          NO_SKIP_OFFSET,
          new UserCompactionTaskGranularityConfig(null, null, true),
          new UserCompactionTaskDimensionsConfig(DimensionsSpec.getDefaultSchemas(ImmutableList.of("language"))),
          null,
          new AggregatorFactory[]{
              new CountAggregatorFactory("ingested_events"),
              new LongSumAggregatorFactory("sum_added", "added"),
              new SketchMergeAggregatorFactory("thetaSketch", "user", 16384, true, false, null),
              new HllSketchBuildAggregatorFactory(
                  "HLLSketchBuild",
                  "user",
                  12,
                  TgtHllType.HLL_4.name(),
                  null,
                  false,
                  false
              ),
              new DoublesSketchAggregatorFactory("quantilesDoublesSketch", "delta", 128, 1000000000L, null)
          },
          false,
          CompactionEngine.NATIVE
      );
      // should now only have 1 row after compaction
      // added = null, count = 3, sum_added = 93
      forceTriggerAutoCompaction(1);

      verifyScanResult("added", "...");
      verifyScanResult("ingested_events", "3");
      verifyScanResult("sum_added", "93");
      verifyScanResult("COUNT(*)", "1");
      verifyDistinctCount("3,3");

      verifySegmentsCompacted(1, MAX_ROWS_PER_SEGMENT_COMPACTED);
      verifySegmentIntervals(intervalsBeforeCompaction);

      List<TaskStatusPlus> compactTasksBefore = getCompleteTasksForDataSource(fullDatasourceName);
      // Verify rollup segments does not get compacted again
      forceTriggerAutoCompaction(1);
      List<TaskStatusPlus> compactTasksAfter = getCompleteTasksForDataSource(fullDatasourceName);
      Assertions.assertEquals(compactTasksAfter.size(), compactTasksBefore.size());
    }
  }

  @Test()
  public void testAutoCompactionOnlyRowsWithoutMetricShouldAddNewMetrics() throws Exception
  {
    // added = 31, count = null, sum_added = null
    loadData(INDEX_TASK_WITHOUT_ROLLUP_FOR_PRESERVE_METRICS);
    // added = 31, count = null, sum_added = null
    loadData(INDEX_TASK_WITHOUT_ROLLUP_FOR_PRESERVE_METRICS);
    try (final Closeable ignored = unloader(fullDatasourceName)) {
      final List<Interval> intervalsBeforeCompaction = getSegmentIntervals();

      // 2 segments across 1 days...
      verifySegmentsCount(2);
      verifyScanResult("added", "31\n31");
      verifyScanResult("COUNT(*)", "2");

      submitCompactionConfig(
          MAX_ROWS_PER_SEGMENT_COMPACTED,
          NO_SKIP_OFFSET,
          new UserCompactionTaskGranularityConfig(null, null, true),
          new UserCompactionTaskDimensionsConfig(DimensionsSpec.getDefaultSchemas(ImmutableList.of("language"))),
          null,
          new AggregatorFactory[] {new CountAggregatorFactory("ingested_events"), new LongSumAggregatorFactory("sum_added", "added")},
          false,
          CompactionEngine.NATIVE
      );
      // should now only have 1 row after compaction
      // added = null, count = 2, sum_added = 62
      forceTriggerAutoCompaction(1);

      verifyScanResult("added", "...");
      verifyScanResult("ingested_events", "2");
      verifyScanResult("sum_added", "62");
      verifyScanResult("COUNT(*)", "1");

      verifySegmentsCompacted(1, MAX_ROWS_PER_SEGMENT_COMPACTED);
      verifySegmentIntervals(intervalsBeforeCompaction);

      List<TaskStatusPlus> compactTasksBefore = getCompleteTasksForDataSource(fullDatasourceName);
      // Verify rollup segments does not get compacted again
      forceTriggerAutoCompaction(1);
      List<TaskStatusPlus> compactTasksAfter = getCompleteTasksForDataSource(fullDatasourceName);
      Assertions.assertEquals(compactTasksAfter.size(), compactTasksBefore.size());
    }
  }

  @MethodSource("getEngine")
  @ParameterizedTest(name = "compactionEngine={0}")
  public void testAutoCompactionWithMetricColumnSameAsInputColShouldOverwriteInputWithMetrics(CompactionEngine engine)
      throws Exception
  {
    // added = 31
    loadData(INDEX_TASK_WITHOUT_ROLLUP_FOR_PRESERVE_METRICS);
    // added = 31
    loadData(INDEX_TASK_WITHOUT_ROLLUP_FOR_PRESERVE_METRICS);
    if (engine == CompactionEngine.MSQ) {
      updateCompactionTaskSlot(0.1, 2);
    }
    try (final Closeable ignored = unloader(fullDatasourceName)) {
      final List<Interval> intervalsBeforeCompaction = getSegmentIntervals();

      // 2 segments across 1 days...
      verifySegmentsCount(2);
      verifyScanResult("added", "31\n31");
      verifyScanResult("COUNT(*)", "2");

      submitCompactionConfig(
          MAX_ROWS_PER_SEGMENT_COMPACTED,
          NO_SKIP_OFFSET,
          new UserCompactionTaskGranularityConfig(null, null, true),
          new UserCompactionTaskDimensionsConfig(DimensionsSpec.getDefaultSchemas(ImmutableList.of("language"))),
          null,
          new AggregatorFactory[] {new LongSumAggregatorFactory("added", "added")},
          false,
          engine
      );
      // should now only have 1 row after compaction
      // added = 62
      forceTriggerAutoCompaction(1);

      verifyScanResult("added", "62");
      verifyScanResult("COUNT(*)", "1");

      verifySegmentsCompacted(1, MAX_ROWS_PER_SEGMENT_COMPACTED);
      verifySegmentIntervals(intervalsBeforeCompaction);

      List<TaskStatusPlus> compactTasksBefore = getCompleteTasksForDataSource(fullDatasourceName);
      // Verify rollup segments does not get compacted again
      forceTriggerAutoCompaction(1);
      List<TaskStatusPlus> compactTasksAfter = getCompleteTasksForDataSource(fullDatasourceName);
      Assertions.assertEquals(compactTasksAfter.size(), compactTasksBefore.size());
    }
  }

  @Test
  public void testAutoCompactionOnlyRowsWithMetricShouldPreserveExistingMetrics() throws Exception
  {
    // added = null, count = 2, sum_added = 62
    loadData(INDEX_TASK_WITH_ROLLUP_FOR_PRESERVE_METRICS);
    // added = null, count = 2, sum_added = 62
    loadData(INDEX_TASK_WITH_ROLLUP_FOR_PRESERVE_METRICS);
    try (final Closeable ignored = unloader(fullDatasourceName)) {
      final List<Interval> intervalsBeforeCompaction = getSegmentIntervals();

      // 2 segments across 1 days...
      verifySegmentsCount(2);
      verifyScanResult("ingested_events", "2\n2");
      verifyScanResult("sum_added", "62\n62");
      verifyScanResult("COUNT(*)", "2");

      submitCompactionConfig(
          MAX_ROWS_PER_SEGMENT_COMPACTED,
          NO_SKIP_OFFSET,
          new UserCompactionTaskGranularityConfig(null, null, true),
          new UserCompactionTaskDimensionsConfig(DimensionsSpec.getDefaultSchemas(ImmutableList.of("language"))),
          null,
          new AggregatorFactory[] {new CountAggregatorFactory("ingested_events"), new LongSumAggregatorFactory("sum_added", "added")},
          false,
          CompactionEngine.NATIVE
      );
      // should now only have 1 row after compaction
      // added = null, count = 4, sum_added = 124
      forceTriggerAutoCompaction(1);

      verifyScanResult("ingested_events", "4");
      verifyScanResult("sum_added", "124");
      verifyScanResult("COUNT(*)", "1");

      verifySegmentsCompacted(1, MAX_ROWS_PER_SEGMENT_COMPACTED);
      verifySegmentIntervals(intervalsBeforeCompaction);

      List<TaskStatusPlus> compactTasksBefore = getCompleteTasksForDataSource(fullDatasourceName);
      // Verify rollup segments does not get compacted again
      forceTriggerAutoCompaction(1);
      List<TaskStatusPlus> compactTasksAfter = getCompleteTasksForDataSource(fullDatasourceName);
      Assertions.assertEquals(compactTasksAfter.size(), compactTasksBefore.size());
    }
  }

  @ParameterizedTest(name = "compactionEngine={0}")
  @MethodSource("getEngine")
  public void testAutoCompactionPreservesCreateBitmapIndexInDimensionSchema(CompactionEngine engine) throws Exception
  {
    loadData(INDEX_TASK);
    try (final Closeable ignored = unloader(fullDatasourceName)) {
      // 4 segments across 2 days (4 total)
      verifySegmentsCount(4);
      verifyQuery(INDEX_QUERIES_RESOURCE);

      LOG.info("Auto compaction test with YEAR segment granularity, dropExisting is true");
      Granularity newSegmentGranularity = Granularities.YEAR;

      List<DimensionSchema> dimensionSchemas = ImmutableList.of(
          new StringDimensionSchema("language", DimensionSchema.MultiValueHandling.SORTED_ARRAY, false),
          new AutoTypeColumnSchema("deleted", ColumnType.DOUBLE)
      );

      submitCompactionConfig(
          MAX_ROWS_PER_SEGMENT_COMPACTED,
          NO_SKIP_OFFSET,
          new UserCompactionTaskGranularityConfig(newSegmentGranularity, null, true),
          new UserCompactionTaskDimensionsConfig(dimensionSchemas),
          null,
          new AggregatorFactory[] {new LongSumAggregatorFactory("added", "added")},
          true,
          engine
      );
      // Compacted into 1 segment for the entire year.
      forceTriggerAutoCompaction(1);
      verifySegmentsCompacted(1, MAX_ROWS_PER_SEGMENT_COMPACTED);
      verifySegmentsCompactedDimensionSchema(dimensionSchemas);
    }
  }

  @MethodSource("getEngine")
  @ParameterizedTest(name = "compactionEngine={0}")
  public void testAutoCompactionRollsUpMultiValueDimensionsWithoutUnnest(CompactionEngine engine) throws Exception
  {
    loadData(INDEX_TASK);
    try (final Closeable ignored = unloader(fullDatasourceName)) {
      // 4 segments across 2 days (4 total)
      verifySegmentsCount(4);
      verifyQuery(INDEX_QUERIES_RESOURCE);

      LOG.info("Auto compaction test with YEAR segment granularity, DAY query granularity, dropExisting is true");

      List<DimensionSchema> dimensionSchemas = ImmutableList.of(
          new StringDimensionSchema("language", null, true),
          new StringDimensionSchema("tags", DimensionSchema.MultiValueHandling.SORTED_ARRAY, true)
      );

      submitCompactionConfig(
          MAX_ROWS_PER_SEGMENT_COMPACTED,
          NO_SKIP_OFFSET,
          new UserCompactionTaskGranularityConfig(Granularities.YEAR, Granularities.DAY, true),
          new UserCompactionTaskDimensionsConfig(dimensionSchemas),
          null,
          new AggregatorFactory[] {new LongSumAggregatorFactory("added", "added")},
          true,
          engine
      );
      // Compacted into 1 segment for the entire year.
      forceTriggerAutoCompaction(1);
      verifyScanResult("COUNT(*)", "1");
      verifyScanResult("added", "516.0");
      verifySegmentsCompacted(1, MAX_ROWS_PER_SEGMENT_COMPACTED);
      verifySegmentsCompactedDimensionSchema(dimensionSchemas);
    }
  }

  @Test
  public void testAutoCompactionDutySubmitAndVerifyCompaction() throws Exception
  {
    loadData(INDEX_TASK);
    try (final Closeable ignored = unloader(fullDatasourceName)) {
      final List<Interval> intervalsBeforeCompaction = getSegmentIntervals();

      // 4 segments across 2 days (4 total)...
      verifySegmentsCount(4);
      verifyQuery(INDEX_QUERIES_RESOURCE);

      submitCompactionConfig(MAX_ROWS_PER_SEGMENT_COMPACTED, Period.days(1), CompactionEngine.NATIVE);
      //...compacted into 1 new segment for 1 day. 1 day compacted and 1 day skipped/remains uncompacted. (3 total)
      forceTriggerAutoCompaction(3);
      verifyQuery(INDEX_QUERIES_RESOURCE);
      verifySegmentsCompacted(1, MAX_ROWS_PER_SEGMENT_COMPACTED);
      verifySegmentIntervals(intervalsBeforeCompaction);
      getAndAssertCompactionStatus(
          fullDatasourceName,
          Matchers.equalTo(0L),
          Matchers.greaterThan(0L),
          Matchers.greaterThan(0L),
          0,
          2,
          2,
          0,
          1,
          1);
      submitCompactionConfig(MAX_ROWS_PER_SEGMENT_COMPACTED, NO_SKIP_OFFSET, CompactionEngine.NATIVE);
      //...compacted into 1 new segment for the remaining one day. 2 day compacted and 0 day uncompacted. (2 total)
      forceTriggerAutoCompaction(2);
      verifyQuery(INDEX_QUERIES_RESOURCE);
      verifySegmentsCompacted(2, MAX_ROWS_PER_SEGMENT_COMPACTED);
      verifySegmentIntervals(intervalsBeforeCompaction);
      getAndAssertCompactionStatus(
          fullDatasourceName,
          Matchers.equalTo(0L),
          Matchers.greaterThan(0L),
          Matchers.equalTo(0L),
          0,
          3,
          0,
          0,
          2,
          0);
    }
  }

  @MethodSource("getEngine")
  @ParameterizedTest(name = "compactionEngine={0}")
  public void testAutoCompactionDutyCanUpdateCompactionConfig(CompactionEngine engine) throws Exception
  {
    loadData(INDEX_TASK);
    try (final Closeable ignored = unloader(fullDatasourceName)) {
      final List<Interval> intervalsBeforeCompaction = getSegmentIntervals();

      // 4 segments across 2 days (4 total)...
      verifySegmentsCount(4);
      verifyQuery(INDEX_QUERIES_RESOURCE);

      // Dummy compaction config which will be overwritten
      submitCompactionConfig(10000, NO_SKIP_OFFSET, engine);
      // New compaction config should overwrites the existing compaction config
      submitCompactionConfig(1, NO_SKIP_OFFSET, engine);

      LOG.info("Auto compaction test with dynamic partitioning");

      // Instead of merging segments, the updated config will split segments!
      //...compacted into 10 new segments across 2 days. 5 new segments each day (10 total)
      forceTriggerAutoCompaction(10);
      verifyQuery(INDEX_QUERIES_RESOURCE);
      verifySegmentsCompacted(10, 1);
      verifySegmentIntervals(intervalsBeforeCompaction);

      if (engine == CompactionEngine.NATIVE) {
        // HashedPartitionsSpec not supported by MSQ.
        LOG.info("Auto compaction test with hash partitioning");

        final HashedPartitionsSpec hashedPartitionsSpec = new HashedPartitionsSpec(null, 3, null);
        submitCompactionConfig(hashedPartitionsSpec, NO_SKIP_OFFSET, null, null, null, null, false, engine);
        // 3 segments for both 2013-08-31 and 2013-09-01. (Note that numShards guarantees max shards but not exact
        // number of final shards, since some shards may end up empty.)
        forceTriggerAutoCompaction(6);
        verifyQuery(INDEX_QUERIES_RESOURCE);
        verifySegmentsCompacted(hashedPartitionsSpec, 6);
        verifySegmentIntervals(intervalsBeforeCompaction);
      }

      LOG.info("Auto compaction test with range partitioning");

      final DimensionRangePartitionsSpec inputRangePartitionsSpec = new DimensionRangePartitionsSpec(
          5,
          null,
          ImmutableList.of("city"),
          false
      );
      DimensionRangePartitionsSpec expectedRangePartitionsSpec = inputRangePartitionsSpec;
      if (engine == CompactionEngine.MSQ) {
        // Range spec is transformed to its effective maxRowsPerSegment equivalent in MSQ
        expectedRangePartitionsSpec = new DimensionRangePartitionsSpec(
            null,
            7,
            ImmutableList.of("city"),
            false
        );
      }
      submitCompactionConfig(inputRangePartitionsSpec, NO_SKIP_OFFSET, null, null, null, null, false, engine);
      forceTriggerAutoCompaction(2);
      verifyQuery(INDEX_QUERIES_RESOURCE);
      verifySegmentsCompacted(expectedRangePartitionsSpec, 2);
      verifySegmentIntervals(intervalsBeforeCompaction);
    }
  }

  @MethodSource("getEngine")
  @ParameterizedTest(name = "compactionEngine={0}")
  public void testAutoCompactionDutyCanDeleteCompactionConfig(CompactionEngine engine) throws Exception
  {
    loadData(INDEX_TASK);
    try (final Closeable ignored = unloader(fullDatasourceName)) {
      final List<Interval> intervalsBeforeCompaction = getSegmentIntervals();

      // 4 segments across 2 days (4 total)...
      verifySegmentsCount(4);
      verifyQuery(INDEX_QUERIES_RESOURCE);

      submitCompactionConfig(MAX_ROWS_PER_SEGMENT_COMPACTED, NO_SKIP_OFFSET, engine);
      deleteCompactionConfig();

      // ...should remains unchanged (4 total)
      forceTriggerAutoCompaction(4);
      verifyQuery(INDEX_QUERIES_RESOURCE);
      verifySegmentsCompacted(0, null);
      // Auto compaction stats should be deleted as compacation config was deleted
      Assertions.assertNull(compactionResource.getCompactionStatus(fullDatasourceName));
      verifySegmentIntervals(intervalsBeforeCompaction);
    }
  }

  @Test
  public void testAutoCompactionDutyCanUpdateTaskSlots() throws Exception
  {
    // Set compactionTaskSlotRatio to 0 to prevent any compaction
    updateCompactionTaskSlot(0, 0);
    loadData(INDEX_TASK);
    try (final Closeable ignored = unloader(fullDatasourceName)) {
      final List<Interval> intervalsBeforeCompaction = getSegmentIntervals();

      // 4 segments across 2 days (4 total)...
      verifySegmentsCount(4);
      verifyQuery(INDEX_QUERIES_RESOURCE);

      submitCompactionConfig(MAX_ROWS_PER_SEGMENT_COMPACTED, NO_SKIP_OFFSET, CompactionEngine.NATIVE);
      // ...should remains unchanged (4 total)
      forceTriggerAutoCompaction(4);
      verifyQuery(INDEX_QUERIES_RESOURCE);
      verifySegmentsCompacted(0, null);
      verifySegmentIntervals(intervalsBeforeCompaction);
      Assertions.assertNull(compactionResource.getCompactionStatus(fullDatasourceName));
      // Update compaction slots to be 1
      updateCompactionTaskSlot(1, 1);
      // One day compacted (1 new segment) and one day remains uncompacted. (3 total)
      forceTriggerAutoCompaction(3);
      verifyQuery(INDEX_QUERIES_RESOURCE);
      verifySegmentsCompacted(1, MAX_ROWS_PER_SEGMENT_COMPACTED);
      verifySegmentIntervals(intervalsBeforeCompaction);
      getAndAssertCompactionStatus(
          fullDatasourceName,
          Matchers.greaterThan(0L),
          Matchers.greaterThan(0L),
          Matchers.equalTo(0L),
          2,
          2,
          0,
          1,
          1,
          0);
      MatcherAssert.assertThat(
          Long.parseLong(compactionResource.getCompactionProgress(fullDatasourceName).get("remainingSegmentSize")),
          Matchers.greaterThan(0L)
      );
      // Run compaction again to compact the remaining day
      // Remaining day compacted (1 new segment). Now both days compacted (2 total)
      forceTriggerAutoCompaction(2);
      verifyQuery(INDEX_QUERIES_RESOURCE);
      verifySegmentsCompacted(2, MAX_ROWS_PER_SEGMENT_COMPACTED);
      verifySegmentIntervals(intervalsBeforeCompaction);
      getAndAssertCompactionStatus(
          fullDatasourceName,
          Matchers.equalTo(0L),
          Matchers.greaterThan(0L),
          Matchers.equalTo(0L),
          0,
          3,
          0,
          0,
          2,
          0);
    }
  }

  @MethodSource("getEngine")
  @ParameterizedTest(name = "compactionEngine={0}")
  @Disabled("Disabling due to timeouts")
  public void testAutoCompactionDutyWithSegmentGranularityAndWithDropExistingTrue(CompactionEngine engine) throws Exception
  {
    // Interval is "2013-08-31/2013-09-02", segment gran is DAY,
    // "maxRowsPerSegment": 3
    // input files:
    //    "/resources/data/batch_index/json/wikipedia_index_data1.json",
    //        3rows -> "2013-08-31T01:02:33Z", "2013-08-31T03:32:45Z", "2013-08-31T07:11:21Z"
    //     "/resources/data/batch_index/json/wikipedia_index_data2.json",
    //       3 rows -> "2013-08-31T11:58:39Z", "2013-08-31T12:41:27Z", "2013-09-01T01:02:33Z"
    //     "/resources/data/batch_index/json/wikipedia_index_data3.json"
    //       4 rows -> "2013-09-01T03:32:45Z", "2013-09-01T07:11:21Z", "2013-09-01T11:58:39Z", "2013-09-01T12:41:27Z"
    //      Summary of data:
    //       5 rows @ 2013-08031 and 5 at 2013-0901, TWO days have data only
    //      Initial load/ingestion: DAY, "intervals" : [ "2013-08-31/2013-09-02" ], Four segments, no tombstones
    //      1st compaction: YEAR: 10 rows during 2013 (4 segments of at most three rows each)
    //              "interval": "2013-01-01T00:00:00.000Z/2014-01-01T00:00:00.000Z",
    //      2nd compaction: MONTH: 5 rows @ 2013-08 (two segments), 5 rows @ 2013-09 (two segments)
    //              "interval": "2013-01-01T00:00:00.000Z/2014-01-01T00:00:00.000Z",
    //                             Four data segments (two months) and 10 tombstones for remaining months
    //      3d compaction: SEMESTER:  5 rows @ 2013-08-31 (two segments), 5 rows @ 2013-09-01 (two segments),
    //               2 compactions were generated for year 2013; one for each semester to be compacted of the whole year.
    //
    loadData(INDEX_TASK);

    try (final Closeable ignored = unloader(fullDatasourceName)) {
      final List<Interval> intervalsBeforeCompaction = getSegmentIntervals();

      // 4 segments across 2 days (4 total)...
      verifySegmentsCount(4);
      verifyQuery(INDEX_QUERIES_RESOURCE);


      LOG.info("Auto compaction test with YEAR segment granularity, dropExisting is true");
      Granularity newGranularity = Granularities.YEAR;
      // Set dropExisting to true
      // "interval": "2013-01-01T00:00:00.000Z/2014-01-01T00:00:00.000Z",
      submitCompactionConfig(
          1000,
          NO_SKIP_OFFSET,
          new UserCompactionTaskGranularityConfig(newGranularity, null, null),
          true,
          engine
      );

      List<Interval> expectedIntervalAfterCompaction =
          EmbeddedClusterApis.createAlignedIntervals(intervalsBeforeCompaction, newGranularity);
      forceTriggerAutoCompaction(1);
      verifyQuery(INDEX_QUERIES_RESOURCE);
      verifySegmentsCompacted(1, 1000);
      verifySegmentIntervals(expectedIntervalAfterCompaction);


      LOG.info("Auto compaction test with MONTH segment granularity, dropExisting is true");
      //  "interval": "2013-01-01T00:00:00.000Z/2014-01-01T00:00:00.000Z",
      newGranularity = Granularities.MONTH;
      // Set dropExisting to true
      submitCompactionConfig(
          1000,
          NO_SKIP_OFFSET,
          new UserCompactionTaskGranularityConfig(newGranularity, null, null),
          true,
          engine
      );

      // Since dropExisting is set to true...
      // Again data is only in two days
      // The earlier segment with YEAR granularity will be completely covered, overshadowed, by the
      // new MONTH segments for data and tombstones for days with no data
      // Hence, we will only have 2013-08 to 2013-09 months with data
      // plus 12 tombstones
      final List<Interval> intervalsAfterYEARCompactionButBeforeMONTHCompaction =
          getSegmentIntervals();
      expectedIntervalAfterCompaction = EmbeddedClusterApis.createAlignedIntervals(
          intervalsAfterYEARCompactionButBeforeMONTHCompaction,
          newGranularity
      );
      forceTriggerAutoCompaction(12);
      verifyQuery(INDEX_QUERIES_RESOURCE);
      verifyTombstones(10);
      verifySegmentsCompacted(12, 1000);
      verifySegmentIntervals(expectedIntervalAfterCompaction);

      LOG.info("Auto compaction test with SEMESTER segment granularity, dropExisting is true, over tombstones");
      // only reason is semester and not quarter or month is to minimize time in the test but to
      // ensure that one of the compactions compacts *only* tombstones. The first semester will
      // compact only tombstones, so it should be a tombstone itself.
      newGranularity = new PeriodGranularity(new Period("P6M"), null, DateTimeZone.UTC);
      // Set dropExisting to true
      submitCompactionConfig(
          1000,
          NO_SKIP_OFFSET,
          new UserCompactionTaskGranularityConfig(newGranularity, null, null),
          true,
          engine
      );

      // Since dropExisting is set to true...
      // The earlier 12 segments with MONTH granularity will be completely covered, overshadowed, by the
      // new PT6M segments for data and tombstones for days with no data
      // Hence, we will have two segments, one tombstone for the first semester and one data segment for the second.
      forceTriggerAutoCompaction(2); // two semesters compacted
      verifyQuery(INDEX_QUERIES_RESOURCE);
      verifyTombstones(1);
      verifySegmentsCompacted(2, 1000);

      expectedIntervalAfterCompaction =
          Arrays.asList(
              Intervals.of("2013-01-01T00:00:00.000Z/2013-07-01T00:00:00.000Z"),
              Intervals.of("2013-07-01T00:00:00.000Z/2014-01-01T00:00:00.000Z")
          );
      verifySegmentIntervals(expectedIntervalAfterCompaction);

      // verify that autocompaction completed  before
      List<TaskStatusPlus> compactTasksBefore = getCompleteTasksForDataSource(fullDatasourceName);
      forceTriggerAutoCompaction(2);
      List<TaskStatusPlus> compactTasksAfter = getCompleteTasksForDataSource(fullDatasourceName);
      Assertions.assertEquals(compactTasksAfter.size(), compactTasksBefore.size());

    }
  }

  @MethodSource("getEngine")
  @ParameterizedTest(name = "compactionEngine={0}")
  @Disabled("Disabling due to timeouts")
  public void testAutoCompactionDutyWithSegmentGranularityAndWithDropExistingTrueThenFalse(CompactionEngine engine) throws Exception
  {
    // Interval is "2013-08-31/2013-09-02", segment gran is DAY,
    // "maxRowsPerSegment": 3
    // input files:
    //    "/resources/data/batch_index/json/wikipedia_index_data1.json",
    //        3rows -> "2013-08-31T01:02:33Z", "2013-08-31T03:32:45Z", "2013-08-31T07:11:21Z"
    //     "/resources/data/batch_index/json/wikipedia_index_data2.json",
    //       3 rows -> "2013-08-31T11:58:39Z", "2013-08-31T12:41:27Z", "2013-09-01T01:02:33Z"
    //     "/resources/data/batch_index/json/wikipedia_index_data3.json"
    //       4 rows -> "2013-09-01T03:32:45Z", "2013-09-01T07:11:21Z", "2013-09-01T11:58:39Z", "2013-09-01T12:41:27Z"
    //      Summary of data:
    //       5 rows @ 2013-08031 and 5 at 2013-0901, TWO days have data only
    //      Initial load/ingestion: DAY, "intervals" : [ "2013-08-31/2013-09-02" ], Four segments, no tombstones
    //      1st compaction: YEAR: 10 rows during 2013 (4 segments of at most three rows each)
    //              "interval": "2013-01-01T00:00:00.000Z/2014-01-01T00:00:00.000Z",
    //      2nd compaction: MONTH: 5 rows @ 2013-08 (two segments), 5 rows @ 2013-09 (two segments)
    //              "interval": "2013-01-01T00:00:00.000Z/2014-01-01T00:00:00.000Z",
    //                             Four data segments (two months) and 10 tombstones for remaining months
    //      3d compaction: SEMESTER:  5 rows @ 2013-08-31, 5 rows @ 2013-09-01 (two segment),
    //               2 compactions were generated for year 2013; one for each semester to be compacted of the whole year.
    //
    loadData(INDEX_TASK);

    try (final Closeable ignored = unloader(fullDatasourceName)) {
      final List<Interval> intervalsBeforeCompaction = getSegmentIntervals();

      // 4 segments across 2 days (4 total)...
      verifySegmentsCount(4);
      verifyQuery(INDEX_QUERIES_RESOURCE);


      LOG.info("Auto compaction test with YEAR segment granularity, dropExisting is true");
      Granularity newGranularity = Granularities.YEAR;
      // Set dropExisting to true
      // "interval": "2013-01-01T00:00:00.000Z/2014-01-01T00:00:00.000Z",
      submitCompactionConfig(
          1000,
          NO_SKIP_OFFSET,
          new UserCompactionTaskGranularityConfig(newGranularity, null, null),
          true,
          engine
      );

      List<Interval> expectedIntervalAfterCompaction =
          EmbeddedClusterApis.createAlignedIntervals(intervalsBeforeCompaction, newGranularity);
      forceTriggerAutoCompaction(1);
      verifyQuery(INDEX_QUERIES_RESOURCE);
      verifySegmentsCompacted(1, 1000);
      verifySegmentIntervals(expectedIntervalAfterCompaction);


      LOG.info("Auto compaction test with MONTH segment granularity, dropExisting is true");
      //  "interval": "2013-01-01T00:00:00.000Z/2014-01-01T00:00:00.000Z",
      newGranularity = Granularities.MONTH;
      // Set dropExisting to true
      submitCompactionConfig(
          1000,
          NO_SKIP_OFFSET,
          new UserCompactionTaskGranularityConfig(newGranularity, null, null),
          true,
          engine
      );

      // Since dropExisting is set to true...
      // Again data is only in two days
      // The earlier segment with YEAR granularity will be completely covered, overshadowed, by the
      // new MONTH segments for data and tombstones for days with no data
      // Hence, we will only have 2013-08 to 2013-09 months with data
      // plus 12 tombstones
      final List<Interval> intervalsAfterYEARCompactionButBeforeMONTHCompaction =
          getSegmentIntervals();
      expectedIntervalAfterCompaction = EmbeddedClusterApis.createAlignedIntervals(
          intervalsAfterYEARCompactionButBeforeMONTHCompaction,
          newGranularity
      );
      forceTriggerAutoCompaction(12);
      verifyQuery(INDEX_QUERIES_RESOURCE);
      verifyTombstones(10);
      verifySegmentsCompacted(12, 1000);
      verifySegmentIntervals(expectedIntervalAfterCompaction);

      // Now compact again over tombstones but with dropExisting set to false:
      LOG.info("Auto compaction test with SEMESTER segment granularity, dropExisting is false, over tombstones");
      newGranularity = new PeriodGranularity(new Period("P6M"), null, DateTimeZone.UTC);
      // Set dropExisting to false
      submitCompactionConfig(
          1000,
          NO_SKIP_OFFSET,
          new UserCompactionTaskGranularityConfig(newGranularity, null, null),
          false,
          engine
      );

      // Since dropExisting is set to false the first semester will be forced to dropExisting true
      // Hence, we will have two, one tombstone for the first semester and one data segment for the second.
      forceTriggerAutoCompaction(2); // two semesters compacted
      verifyQuery(INDEX_QUERIES_RESOURCE);
      verifyTombstones(1);
      verifySegmentsCompacted(2, 1000);

      expectedIntervalAfterCompaction =
          Arrays.asList(
              Intervals.of("2013-01-01T00:00:00.000Z/2013-07-01T00:00:00.000Z"),
              Intervals.of("2013-07-01T00:00:00.000Z/2014-01-01T00:00:00.000Z")
          );
      verifySegmentIntervals(expectedIntervalAfterCompaction);

      // verify that autocompaction completed  before
      List<TaskStatusPlus> compactTasksBefore = getCompleteTasksForDataSource(fullDatasourceName);
      forceTriggerAutoCompaction(2);
      List<TaskStatusPlus> compactTasksAfter = getCompleteTasksForDataSource(fullDatasourceName);
      Assertions.assertEquals(compactTasksAfter.size(), compactTasksBefore.size());
    }
  }

  @Test
  public void testAutoCompactionDutyWithSegmentGranularityAndWithDropExistingFalse() throws Exception
  {
    loadData(INDEX_TASK);
    try (final Closeable ignored = unloader(fullDatasourceName)) {
      final List<Interval> intervalsBeforeCompaction = getSegmentIntervals();

      // 4 segments across 2 days (4 total)...
      verifySegmentsCount(4);
      verifyQuery(INDEX_QUERIES_RESOURCE);

      Granularity newGranularity = Granularities.YEAR;
      // Set dropExisting to false
      submitCompactionConfig(
          1000,
          NO_SKIP_OFFSET,
          new UserCompactionTaskGranularityConfig(newGranularity, null, null),
          false,
          CompactionEngine.NATIVE
      );

      LOG.info("Auto compaction test with YEAR segment granularity");

      List<Interval> expectedIntervalAfterCompaction
          = EmbeddedClusterApis.createAlignedIntervals(intervalsBeforeCompaction, newGranularity);
      forceTriggerAutoCompaction(1);
      verifyQuery(INDEX_QUERIES_RESOURCE);
      verifySegmentsCompacted(1, 1000);
      verifySegmentIntervals(expectedIntervalAfterCompaction);

      newGranularity = Granularities.DAY;
      // Set dropExisting to false
      submitCompactionConfig(
          1000,
          NO_SKIP_OFFSET,
          new UserCompactionTaskGranularityConfig(newGranularity, null, null),
          false,
          CompactionEngine.NATIVE
      );

      LOG.info("Auto compaction test with DAY segment granularity");

      // Since dropExisting is set to false...
      // The earlier segment with YEAR granularity is still 'used' as it’s not fully overshaowed.
      // This is because we only have newer version on 2013-08-31 to 2013-09-01 and 2013-09-01 to 2013-09-02.
      // The version for the YEAR segment is still the latest for 2013-01-01 to 2013-08-31 and 2013-09-02 to 2014-01-01.
      // Hence, all three segments are available and the expected intervals are combined from the DAY and YEAR segment granularities
      // (which are 2013-08-31 to 2013-09-01, 2013-09-01 to 2013-09-02 and 2013-01-01 to 2014-01-01)
      expectedIntervalAfterCompaction.addAll(
          EmbeddedClusterApis.createAlignedIntervals(intervalsBeforeCompaction, newGranularity)
      );
      forceTriggerAutoCompaction(3);
      verifyQuery(INDEX_QUERIES_RESOURCE);
      verifySegmentsCompacted(3, 1000);
      verifySegmentIntervals(expectedIntervalAfterCompaction);
    }
  }

  @MethodSource("getEngine")
  @ParameterizedTest(name = "compactionEngine={0}")
  public void testAutoCompactionDutyWithSegmentGranularityAndMixedVersion(CompactionEngine engine) throws Exception
  {
    loadData(INDEX_TASK);
    try (final Closeable ignored = unloader(fullDatasourceName)) {
      final List<Interval> intervalsBeforeCompaction = getSegmentIntervals();

      // 4 segments across 2 days (4 total)...
      verifySegmentsCount(4);
      verifyQuery(INDEX_QUERIES_RESOURCE);

      submitCompactionConfig(MAX_ROWS_PER_SEGMENT_COMPACTED, Period.days(1), engine);
      //...compacted into 1 new segment for 1 day. 1 day compacted and 1 day skipped/remains uncompacted. (3 total)
      forceTriggerAutoCompaction(3);
      verifyQuery(INDEX_QUERIES_RESOURCE);
      verifySegmentsCompacted(1, MAX_ROWS_PER_SEGMENT_COMPACTED);

      Granularity newGranularity = Granularities.YEAR;
      submitCompactionConfig(1000, NO_SKIP_OFFSET, new UserCompactionTaskGranularityConfig(newGranularity, null, null), engine);

      LOG.info("Auto compaction test with YEAR segment granularity");

      List<Interval> expectedIntervalAfterCompaction =
          EmbeddedClusterApis.createAlignedIntervals(intervalsBeforeCompaction, newGranularity);
      // Since the new segmentGranularity is YEAR, it will have mixed versions inside the same time chunk
      // There will be an old version (for the first day interval) from the initial ingestion and
      // a newer version (for the second day interval) from the first compaction
      forceTriggerAutoCompaction(1);
      verifyQuery(INDEX_QUERIES_RESOURCE);
      verifySegmentsCompacted(1, 1000);
      verifySegmentIntervals(expectedIntervalAfterCompaction);
    }
  }

  @MethodSource("getEngine")
  @ParameterizedTest(name = "compactionEngine={0}")
  public void testAutoCompactionDutyWithSegmentGranularityAndExistingCompactedSegmentsHaveSameSegmentGranularity(CompactionEngine engine) throws Exception
  {
    loadData(INDEX_TASK);
    try (final Closeable ignored = unloader(fullDatasourceName)) {
      // 4 segments across 2 days (4 total)...
      verifySegmentsCount(4);
      verifyQuery(INDEX_QUERIES_RESOURCE);

      // Compacted without SegmentGranularity in auto compaction config
      submitCompactionConfig(MAX_ROWS_PER_SEGMENT_COMPACTED, NO_SKIP_OFFSET, engine);
      forceTriggerAutoCompaction(2);
      verifyQuery(INDEX_QUERIES_RESOURCE);
      verifySegmentsCompacted(2, MAX_ROWS_PER_SEGMENT_COMPACTED);

      List<TaskStatusPlus> compactTasksBefore = getCompleteTasksForDataSource(fullDatasourceName);

      // Segments were compacted and already has DAY granularity since it was initially ingested with DAY granularity.
      // Now set auto compaction with DAY granularity in the granularitySpec
      Granularity newGranularity = Granularities.DAY;
      submitCompactionConfig(MAX_ROWS_PER_SEGMENT_COMPACTED, NO_SKIP_OFFSET, new UserCompactionTaskGranularityConfig(newGranularity, null, null), engine);
      forceTriggerAutoCompaction(2);
      verifyQuery(INDEX_QUERIES_RESOURCE);
      verifySegmentsCompacted(2, MAX_ROWS_PER_SEGMENT_COMPACTED);
      // should be no new compaction task as segmentGranularity is already DAY
      List<TaskStatusPlus> compactTasksAfter = getCompleteTasksForDataSource(fullDatasourceName);
      Assertions.assertEquals(compactTasksAfter.size(), compactTasksBefore.size());
    }
  }

  @MethodSource("getEngine")
  @ParameterizedTest(name = "compactionEngine={0}")
  public void testAutoCompactionDutyWithSegmentGranularityAndExistingCompactedSegmentsHaveDifferentSegmentGranularity(CompactionEngine engine) throws Exception
  {
    loadData(INDEX_TASK);
    try (final Closeable ignored = unloader(fullDatasourceName)) {
      // 4 segments across 2 days (4 total)...
      verifySegmentsCount(4);
      verifyQuery(INDEX_QUERIES_RESOURCE);

      // Compacted without SegmentGranularity in auto compaction config
      submitCompactionConfig(MAX_ROWS_PER_SEGMENT_COMPACTED, NO_SKIP_OFFSET, engine);
      forceTriggerAutoCompaction(2);
      verifyQuery(INDEX_QUERIES_RESOURCE);
      verifySegmentsCompacted(2, MAX_ROWS_PER_SEGMENT_COMPACTED);

      List<TaskStatusPlus> compactTasksBefore = getCompleteTasksForDataSource(fullDatasourceName);

      // Segments were compacted and already has DAY granularity since it was initially ingested with DAY granularity.
      // Now set auto compaction with DAY granularity in the granularitySpec
      Granularity newGranularity = Granularities.YEAR;
      submitCompactionConfig(MAX_ROWS_PER_SEGMENT_COMPACTED, NO_SKIP_OFFSET, new UserCompactionTaskGranularityConfig(newGranularity, null, null), engine);
      forceTriggerAutoCompaction(1);
      verifyQuery(INDEX_QUERIES_RESOURCE);
      verifySegmentsCompacted(1, MAX_ROWS_PER_SEGMENT_COMPACTED);

      // There should be new compaction tasks since SegmentGranularity changed from DAY to YEAR
      List<TaskStatusPlus> compactTasksAfter = getCompleteTasksForDataSource(fullDatasourceName);
      Assertions.assertTrue(compactTasksAfter.size() > compactTasksBefore.size());
    }
  }

  @MethodSource("getEngine")
  @ParameterizedTest(name = "compactionEngine={0}")
  @Disabled("Disabling due to timeouts")
  public void testAutoCompactionDutyWithSegmentGranularityAndSmallerSegmentGranularityCoveringMultipleSegmentsInTimelineAndDropExistingTrue(CompactionEngine engine) throws Exception
  {
    loadData(INDEX_TASK);
    try (final Closeable ignored = unloader(fullDatasourceName)) {
      final List<Interval> intervalsBeforeCompaction = getSegmentIntervals();

      // 4 segments across 2 days (4 total)...
      verifySegmentsCount(4);
      verifyQuery(INDEX_QUERIES_RESOURCE);

      Granularity newGranularity = Granularities.YEAR;
      // Set dropExisting to true
      submitCompactionConfig(MAX_ROWS_PER_SEGMENT_COMPACTED, NO_SKIP_OFFSET, new UserCompactionTaskGranularityConfig(newGranularity, null, null), true, engine);

      List<Interval> expectedIntervalAfterCompaction =
          EmbeddedClusterApis.createAlignedIntervals(intervalsBeforeCompaction, newGranularity);
      // We will still have one visible segment with interval of 2013-01-01/2014-01-01 (compacted with YEAR)
      // and four overshadowed segments
      forceTriggerAutoCompaction(1);
      verifyQuery(INDEX_QUERIES_RESOURCE);
      verifySegmentsCompacted(1, MAX_ROWS_PER_SEGMENT_COMPACTED);
      verifySegmentIntervals(expectedIntervalAfterCompaction);

      loadData(INDEX_TASK);
      verifySegmentsCount(5);
      verifyQuery(INDEX_QUERIES_RESOURCE);
      // 5 segments. 1 compacted YEAR segment and 4 newly ingested DAY segments across 2 days
      // We wil have one segment with interval of 2013-01-01/2014-01-01 (compacted with YEAR) from the compaction earlier
      // two segments with interval of 2013-08-31/2013-09-01 (newly ingested with DAY)
      // and two segments with interval of 2013-09-01/2013-09-02 (newly ingested with DAY)
      expectedIntervalAfterCompaction.addAll(intervalsBeforeCompaction);
      verifySegmentIntervals(expectedIntervalAfterCompaction);

      newGranularity = Granularities.MONTH;
      final List<Interval> intervalsAfterYEARButBeforeMONTHCompaction =
          getSegmentIntervals();
      // Since dropExisting is set to true...
      // This will submit a single compaction task for interval of 2013-01-01/2014-01-01 with MONTH granularity
      submitCompactionConfig(MAX_ROWS_PER_SEGMENT_COMPACTED, NO_SKIP_OFFSET, new UserCompactionTaskGranularityConfig(newGranularity, null, null), true, engine);
      // verify:
      expectedIntervalAfterCompaction = EmbeddedClusterApis.createAlignedIntervals(
          intervalsAfterYEARButBeforeMONTHCompaction,
          Granularities.MONTH
      );
      // The previous segment with interval of 2013-01-01/2014-01-01 (compacted with YEAR) will be
      // completely overshadowed by a combination of tombstones and segments with data.
      // We will only have one segment with interval of 2013-08-01/2013-09-01 (compacted with MONTH)
      // and one segment with interval of 2013-09-01/2013-10-01 (compacted with MONTH)
      // plus ten tombstones for the remaining months, thus expecting 12 intervals...
      forceTriggerAutoCompaction(12);
      verifyQuery(INDEX_QUERIES_RESOURCE);
      verifyTombstones(10);
      verifySegmentsCompacted(12, MAX_ROWS_PER_SEGMENT_COMPACTED);
      verifySegmentIntervals(expectedIntervalAfterCompaction);
    }
  }

  @Test
  @Disabled("Disabling due to timeouts")
  public void testAutoCompactionDutyWithSegmentGranularityAndSmallerSegmentGranularityCoveringMultipleSegmentsInTimelineAndDropExistingFalse() throws Exception
  {
    loadData(INDEX_TASK);
    try (final Closeable ignored = unloader(fullDatasourceName)) {
      final List<Interval> intervalsBeforeCompaction = getSegmentIntervals();

      // 4 segments across 2 days (4 total)...
      verifySegmentsCount(4);
      verifyQuery(INDEX_QUERIES_RESOURCE);

      Granularity newGranularity = Granularities.YEAR;
      // Set dropExisting to false
      submitCompactionConfig(
          MAX_ROWS_PER_SEGMENT_COMPACTED,
          NO_SKIP_OFFSET,
          new UserCompactionTaskGranularityConfig(newGranularity, null, null),
          false,
          CompactionEngine.NATIVE
      );

      List<Interval> expectedIntervalAfterCompaction =
          EmbeddedClusterApis.createAlignedIntervals(intervalsBeforeCompaction, newGranularity);
      // We wil have one segment with interval of 2013-01-01/2014-01-01 (compacted with YEAR)
      forceTriggerAutoCompaction(1);
      verifyQuery(INDEX_QUERIES_RESOURCE);
      verifySegmentsCompacted(1, MAX_ROWS_PER_SEGMENT_COMPACTED);
      verifySegmentIntervals(expectedIntervalAfterCompaction);

      loadData(INDEX_TASK);
      verifySegmentsCount(5);
      verifyQuery(INDEX_QUERIES_RESOURCE);
      // 5 segments. 1 compacted YEAR segment and 4 newly ingested DAY segments across 2 days
      // We wil have one segment with interval of 2013-01-01/2014-01-01 (compacted with YEAR) from the compaction earlier
      // two segments with interval of 2013-08-31/2013-09-01 (newly ingested with DAY)
      // and two segments with interval of 2013-09-01/2013-09-02 (newly ingested with DAY)
      expectedIntervalAfterCompaction.addAll(intervalsBeforeCompaction);
      verifySegmentIntervals(expectedIntervalAfterCompaction);

      newGranularity = Granularities.MONTH;
      // Set dropExisting to false
      submitCompactionConfig(
          MAX_ROWS_PER_SEGMENT_COMPACTED,
          NO_SKIP_OFFSET,
          new UserCompactionTaskGranularityConfig(newGranularity, null, null),
          false,
          CompactionEngine.NATIVE
      );
      // Since dropExisting is set to true...
      // This will submit a single compaction task for interval of 2013-01-01/2014-01-01 with MONTH granularity
      expectedIntervalAfterCompaction =
          EmbeddedClusterApis.createAlignedIntervals(intervalsBeforeCompaction, Granularities.YEAR);
      // Since dropExisting is set to false...
      // We wil have one segment with interval of 2013-01-01/2014-01-01 (compacted with YEAR) from before the compaction
      // one segments with interval of 2013-09-01/2013-10-01 (compacted with MONTH)
      // and one segments with interval of 2013-10-01/2013-11-01 (compacted with MONTH)
      expectedIntervalAfterCompaction.addAll(
          EmbeddedClusterApis.createAlignedIntervals(intervalsBeforeCompaction, Granularities.MONTH)
      );

      forceTriggerAutoCompaction(3);
      verifyQuery(INDEX_QUERIES_RESOURCE);
      verifySegmentsCompacted(3, MAX_ROWS_PER_SEGMENT_COMPACTED);
      verifySegmentIntervals(expectedIntervalAfterCompaction);
    }
  }

  @Test
  public void testAutoCompactionDutyWithSegmentGranularityFinerAndNotAlignWithSegment() throws Exception
  {
    updateCompactionTaskSlot(1, 1);
    final ISOChronology chrono = ISOChronology.getInstance(DateTimes.inferTzFromString("America/Los_Angeles"));
    GranularitySpec specs = new UniformGranularitySpec(Granularities.MONTH, Granularities.DAY, false, List.of(new Interval("2013-08-31/2013-09-02", chrono)));
    loadData(INDEX_TASK_WITH_GRANULARITY_SPEC, specs);
    try (final Closeable ignored = unloader(fullDatasourceName)) {
      verifyScanResult("added", "57.0\n459.0");
      verifyScanResult("COUNT(*)", "2");
      submitCompactionConfig(
          MAX_ROWS_PER_SEGMENT_COMPACTED,
          NO_SKIP_OFFSET,
          new UserCompactionTaskGranularityConfig(Granularities.WEEK, null, null),
          false,
          CompactionEngine.NATIVE
      );
      // Before compaction, we have segments with the interval 2013-08-01/2013-09-01 and 2013-09-01/2013-10-01
      // We will compact the latest segment, 2013-09-01/2013-10-01, to WEEK.
      // Since the start of the week does not align with 2013-09-01 or 2013-10-01, we expect the compaction task's
      // interval to be adjusted so that the compacted WEEK segments does not unintentionally remove data of the
      // non compacted 2013-08-01/2013-09-01 segment.
      // Note that the compacted WEEK segment does not fully cover the original MONTH segment as the MONTH segment
      // does not have data on every week on the month
      forceTriggerAutoCompaction(3);
      // Make sure that no data is lost after compaction
      verifyScanResult("added", "57.0\n459.0");
      verifyScanResult("COUNT(*)", "2");
      verifySegmentsCompacted(1, MAX_ROWS_PER_SEGMENT_COMPACTED);
      List<TaskStatusPlus> tasks = getCompleteTasksForDataSource(fullDatasourceName);
      TaskStatusPlus compactTask = null;
      for (TaskStatusPlus task : tasks) {
        if ("compact".equals(task.getType())) {
          compactTask = task;
        }
      }
      Assertions.assertNotNull(compactTask);
      TaskPayloadResponse task = getTaskPayload(compactTask.getId());
      // Verify that compaction task interval is adjusted to align with segmentGranularity
      Assertions.assertEquals(
          Intervals.of("2013-08-26T00:00:00.000Z/2013-10-07T00:00:00.000Z"),
          (((ClientCompactionTaskQuery) task.getPayload()).getIoConfig().getInputSpec()).getInterval()
      );
    }
  }

  @MethodSource("getEngine")
  @ParameterizedTest(name = "compactionEngine={0}")
  public void testAutoCompactionDutyWithSegmentGranularityCoarserAndNotAlignWithSegment(CompactionEngine engine) throws Exception
  {
    updateCompactionTaskSlot(1, 1);
    final ISOChronology chrono = ISOChronology.getInstance(DateTimes.inferTzFromString("America/Los_Angeles"));
    GranularitySpec specs = new UniformGranularitySpec(Granularities.WEEK, Granularities.DAY, false, List.of(new Interval("2013-08-31/2013-09-02", chrono)));
    loadData(INDEX_TASK_WITH_GRANULARITY_SPEC, specs);
    try (final Closeable ignored = unloader(fullDatasourceName)) {
      verifyScanResult("added", "57.0\n459.0");
      verifyScanResult("COUNT(*)", "2");
      submitCompactionConfig(
          MAX_ROWS_PER_SEGMENT_COMPACTED,
          NO_SKIP_OFFSET,
          new UserCompactionTaskGranularityConfig(Granularities.MONTH, null, null),
          false,
          engine
      );
      // Before compaction, we have segments with the interval 2013-08-26T00:00:00.000Z/2013-09-02T00:00:00.000Z
      // We will compact the latest segment to MONTH.
      // Although the segments before compaction only cover 2013-08-26 to 2013-09-02,
      // we expect the compaction task's interval to align with the MONTH segmentGranularity (2013-08-01 to 2013-10-01)
      forceTriggerAutoCompaction(2);
      // Make sure that no data is lost after compaction
      verifyScanResult("added", "57.0\n459.0");
      verifyScanResult("COUNT(*)", "2");
      verifySegmentsCompacted(2, MAX_ROWS_PER_SEGMENT_COMPACTED);
      List<TaskStatusPlus> tasks = getCompleteTasksForDataSource(fullDatasourceName);
      TaskStatusPlus compactTask = null;
      for (TaskStatusPlus task : tasks) {
        if ("compact".equals(task.getType())) {
          compactTask = task;
        }
      }
      Assertions.assertNotNull(compactTask);
      TaskPayloadResponse task = getTaskPayload(compactTask.getId());
      // Verify that compaction task interval is adjusted to align with segmentGranularity
      Assertions.assertEquals(
          Intervals.of("2013-08-01T00:00:00.000Z/2013-10-01T00:00:00.000Z"),
          (((ClientCompactionTaskQuery) task.getPayload()).getIoConfig().getInputSpec()).getInterval()
      );
    }
  }

  @Test()
  public void testAutoCompactionDutyWithRollup() throws Exception
  {
    final ISOChronology chrono = ISOChronology.getInstance(DateTimes.inferTzFromString("America/Los_Angeles"));
    GranularitySpec specs = new UniformGranularitySpec(Granularities.DAY, Granularities.DAY, false, List.of(new Interval("2013-08-31/2013-09-02", chrono)));
    loadData(INDEX_TASK_WITH_GRANULARITY_SPEC, specs);
    try (final Closeable ignored = unloader(fullDatasourceName)) {
      verifyScanResult("added", "57.0\n459.0");
      verifyScanResult("COUNT(*)", "2");
      submitCompactionConfig(
          MAX_ROWS_PER_SEGMENT_COMPACTED,
          NO_SKIP_OFFSET,
          new UserCompactionTaskGranularityConfig(null, null, true),
          false,
          CompactionEngine.NATIVE
      );
      forceTriggerAutoCompaction(2);
      verifyScanResult("added", "516.0");
      verifyScanResult("COUNT(*)", "1");
      verifySegmentsCompacted(2, MAX_ROWS_PER_SEGMENT_COMPACTED);

      List<TaskStatusPlus> compactTasksBefore = getCompleteTasksForDataSource(fullDatasourceName);
      // Verify rollup segments does not get compacted again
      forceTriggerAutoCompaction(2);
      List<TaskStatusPlus> compactTasksAfter = getCompleteTasksForDataSource(fullDatasourceName);
      Assertions.assertEquals(compactTasksAfter.size(), compactTasksBefore.size());
    }
  }

  @ParameterizedTest(name = "compactionEngine={0}")
  @MethodSource("getEngine")
  public void testAutoCompactionDutyWithQueryGranularity(CompactionEngine engine) throws Exception
  {
    final ISOChronology chrono = ISOChronology.getInstance(DateTimes.inferTzFromString("America/Los_Angeles"));
    GranularitySpec specs = new UniformGranularitySpec(Granularities.DAY, Granularities.NONE, true, List.of(new Interval("2013-08-31/2013-09-02", chrono)));
    loadData(INDEX_TASK_WITH_GRANULARITY_SPEC, specs);
    try (final Closeable ignored = unloader(fullDatasourceName)) {
      verifyScanResult("added", "57.0\n459.0");
      verifyScanResult("COUNT(*)", "2");
      submitCompactionConfig(
          MAX_ROWS_PER_SEGMENT_COMPACTED,
          NO_SKIP_OFFSET,
          new UserCompactionTaskGranularityConfig(null, Granularities.DAY, null),
          false,
          engine
      );
      forceTriggerAutoCompaction(2);
      verifyScanResult("added", "516.0");
      verifyScanResult("COUNT(*)", "1");
      verifySegmentsCompacted(2, MAX_ROWS_PER_SEGMENT_COMPACTED);

      List<TaskStatusPlus> compactTasksBefore = getCompleteTasksForDataSource(fullDatasourceName);
      // Verify rollup segments does not get compacted again
      forceTriggerAutoCompaction(2);
      List<TaskStatusPlus> compactTasksAfter = getCompleteTasksForDataSource(fullDatasourceName);
      Assertions.assertEquals(compactTasksAfter.size(), compactTasksBefore.size());
    }
  }

  @MethodSource("getEngine")
  @ParameterizedTest(name = "compactionEngine={0}")
  public void testAutoCompactionDutyWithDimensionsSpec(CompactionEngine engine) throws Exception
  {
    // Index data with dimensions "page", "language", "user", "unpatrolled", "newPage", "robot", "anonymous",
    // "namespace", "continent", "country", "region", "city"
    loadData(INDEX_TASK_WITH_DIMENSION_SPEC);
    try (final Closeable ignored = unloader(fullDatasourceName)) {
      // 4 segments across 2 days (4 total)...
      verifySegmentsCount(4);

      // Result is not rollup
      verifyScanResult("added", "57.0\n459.0");
      verifyScanResult("COUNT(*)", "2");

      // Compact and change dimension to only "language"
      submitCompactionConfig(
          MAX_ROWS_PER_SEGMENT_COMPACTED,
          NO_SKIP_OFFSET,
          null,
          new UserCompactionTaskDimensionsConfig(DimensionsSpec.getDefaultSchemas(ImmutableList.of("language"))),
          null,
          null,
          false,
          engine
      );
      forceTriggerAutoCompaction(2);

      // Result should rollup on language dimension
      verifyScanResult("added", "516.0");
      verifyScanResult("COUNT(*)", "1");
      verifySegmentsCompacted(2, MAX_ROWS_PER_SEGMENT_COMPACTED);

      List<TaskStatusPlus> compactTasksBefore = getCompleteTasksForDataSource(fullDatasourceName);
      // Verify compacted segments does not get compacted again
      forceTriggerAutoCompaction(2);
      List<TaskStatusPlus> compactTasksAfter = getCompleteTasksForDataSource(fullDatasourceName);
      Assertions.assertEquals(compactTasksAfter.size(), compactTasksBefore.size());
    }
  }

  @ValueSource(booleans = {false})
  @ParameterizedTest(name = "useSupervisors={0}")
  public void testAutoCompactionDutyWithFilter(boolean useSupervisors) throws Exception
  {
    updateClusterConfig(new ClusterCompactionConfig(0.5, 10, null, useSupervisors, null));

    loadData(INDEX_TASK);
    try (final Closeable ignored = unloader(fullDatasourceName)) {
      final List<Interval> intervalsBeforeCompaction = getSegmentIntervals();
      intervalsBeforeCompaction.sort(Comparators.intervalsByStartThenEnd().reversed());
      // 4 segments across 2 days (4 total)...
      verifySegmentsCount(4);

      // Result is not rollup
      // For dim "page", result has values "Gypsy Danger" and "Striker Eureka"
      verifyScanResult("added", "57.0\n459.0");
      verifyScanResult("COUNT(*)", "2");

      // Compact and filter with selector on dim "page" and value "Striker Eureka"
      submitCompactionConfig(
          MAX_ROWS_PER_SEGMENT_COMPACTED,
          NO_SKIP_OFFSET,
          null,
          null,
          new CompactionTransformSpec(new SelectorDimFilter("page", "Striker Eureka", null)),
          null,
          false,
          CompactionEngine.NATIVE
      );
      forceTriggerAutoCompaction(intervalsBeforeCompaction, useSupervisors, 2);

      // For dim "page", result should only contain value "Striker Eureka"
      verifyScanResult("added", "459.0");
      verifyScanResult("COUNT(*)", "1");
      verifySegmentsCompacted(2, MAX_ROWS_PER_SEGMENT_COMPACTED);

      List<TaskStatusPlus> compactTasksBefore = getCompleteTasksForDataSource(fullDatasourceName);
      // Verify compacted segments does not get compacted again
      forceTriggerAutoCompaction(intervalsBeforeCompaction, useSupervisors, 2);
      List<TaskStatusPlus> compactTasksAfter = getCompleteTasksForDataSource(fullDatasourceName);
      Assertions.assertEquals(compactTasksAfter.size(), compactTasksBefore.size());
    }
  }

  @ValueSource(booleans = {false})
  @ParameterizedTest(name = "useSupervisors={0}")
  public void testAutoCompationDutyWithMetricsSpec(boolean useSupervisors) throws Exception
  {
    updateClusterConfig(new ClusterCompactionConfig(0.5, 10, null, useSupervisors, null));

    loadData(INDEX_TASK);
    try (final Closeable ignored = unloader(fullDatasourceName)) {
      final List<Interval> intervalsBeforeCompaction = getSegmentIntervals();
      intervalsBeforeCompaction.sort(Comparators.intervalsByStartThenEnd().reversed());
      // 4 segments across 2 days (4 total)...
      verifySegmentsCount(4);

      // For dim "page", result has values "Gypsy Danger" and "Striker Eureka"
      verifyScanResult("added", "57.0\n459.0");
      verifyScanResult("COUNT(*)", "2");

      // Compact and add longSum and doubleSum metrics
      submitCompactionConfig(
          MAX_ROWS_PER_SEGMENT_COMPACTED,
          NO_SKIP_OFFSET,
          null,
          null,
          null,
          new AggregatorFactory[] {new DoubleSumAggregatorFactory("double_sum_added", "added"), new LongSumAggregatorFactory("long_sum_added", "added")},
          false,
          CompactionEngine.NATIVE
      );
      forceTriggerAutoCompaction(intervalsBeforeCompaction, useSupervisors, 2);

      // Result should be the same with the addition of new metrics, "double_sum_added" and "long_sum_added".
      // These new metrics should have the same value as the input field "added"
      verifyScanResult("double_sum_added", "57.0\n459.0");
      verifyScanResult("long_sum_added", "57\n459");

      verifySegmentsCompacted(2, MAX_ROWS_PER_SEGMENT_COMPACTED);

      List<TaskStatusPlus> compactTasksBefore = getCompleteTasksForDataSource(fullDatasourceName);
      // Verify compacted segments does not get compacted again
      forceTriggerAutoCompaction(intervalsBeforeCompaction, useSupervisors, 2);
      List<TaskStatusPlus> compactTasksAfter = getCompleteTasksForDataSource(fullDatasourceName);
      Assertions.assertEquals(compactTasksAfter.size(), compactTasksBefore.size());
    }
  }

  @Test
  public void testAutoCompactionDutyWithOverlappingInterval() throws Exception
  {
    final ISOChronology chrono = ISOChronology.getInstance(DateTimes.inferTzFromString("America/Los_Angeles"));
    // Create WEEK segment with 2013-08-26 to 2013-09-02
    GranularitySpec specs = new UniformGranularitySpec(Granularities.WEEK, Granularities.NONE, false, List.of(new Interval("2013-08-31/2013-09-02", chrono)));
    loadData(INDEX_TASK_WITH_GRANULARITY_SPEC, specs);
    // Create MONTH segment with 2013-09-01 to 2013-10-01
    specs = new UniformGranularitySpec(Granularities.MONTH, Granularities.NONE, false, List.of(new Interval("2013-09-01/2013-09-02", chrono)));
    loadData(INDEX_TASK_WITH_GRANULARITY_SPEC, specs);

    try (final Closeable ignored = unloader(fullDatasourceName)) {
      verifySegmentsCount(2);

      // Result is not rollup
      // For dim "page", result has values "Gypsy Danger" and "Striker Eureka"
      verifyScanResult("added", "57.0\n459.0");
      verifyScanResult("COUNT(*)", "2");

      submitCompactionConfig(
          MAX_ROWS_PER_SEGMENT_COMPACTED,
          NO_SKIP_OFFSET,
          null,
          null,
          null,
          null,
          false,
          CompactionEngine.NATIVE
      );
      // Compact the MONTH segment
      forceTriggerAutoCompaction(2);
      verifyScanResult("added", "57.0\n459.0");
      verifyScanResult("COUNT(*)", "2");

      // Compact the WEEK segment
      forceTriggerAutoCompaction(2);
      verifyScanResult("added", "57.0\n459.0");
      verifyScanResult("COUNT(*)", "2");

      // Verify all task succeed
      List<TaskStatusPlus> compactTasksBefore = getCompleteTasksForDataSource(fullDatasourceName);
      for (TaskStatusPlus taskResponseObject : compactTasksBefore) {
        Assertions.assertEquals(TaskState.SUCCESS, taskResponseObject.getStatusCode());
      }

      // Verify compacted segments does not get compacted again
      forceTriggerAutoCompaction(2);
      List<TaskStatusPlus> compactTasksAfter = getCompleteTasksForDataSource(fullDatasourceName);
      Assertions.assertEquals(compactTasksAfter.size(), compactTasksBefore.size());
    }
  }

  private <T extends TaskBuilder.IndexCommon<?, ?, ?, ?>> void loadData(Supplier<T> updatePayload)
  {
    loadData(updatePayload, null);
  }

  private <T extends TaskBuilder.IndexCommon<?, ?, ?, ?>> void loadData(
      Supplier<T> taskPayloadSupplier,
      GranularitySpec granularitySpec
  )
  {
    final TaskBuilder.IndexCommon<?, ?, ?, ?> taskBuilder = taskPayloadSupplier.get();
    taskBuilder.dataSource(fullDatasourceName);
    if (granularitySpec != null) {
      taskBuilder.granularitySpec(granularitySpec);
    }

    runTask(taskBuilder);
  }

  private void verifyDistinctCount(String result)
  {
    cluster.callApi().verifySqlQuery(
        Resources.Query.SELECT_APPROX_COUNT_DISTINCT,
        dataSource,
        result
    );
  }

  /**
   * Verifies the result of a SELECT query
   *
   * @param field  Field to select
   * @param result CSV result with special string {@code ...} to represent an empty string.
   */
  private void verifyScanResult(String field, String result)
  {
    final String sql = StringUtils.format(
        "SELECT %s FROM %s WHERE \"language\" = 'en' AND __time < '2013-09-01'",
        field, dataSource
    );

    // replace empty placeholder with empty string
    result = StringUtils.replace(result, "...", "\"\"");

    Assertions.assertEquals(
        result,
        cluster.runSql(sql),
        StringUtils.format("Query[%s] failed", sql)
    );
  }

  private void updateClusterConfig(ClusterCompactionConfig clusterConfig)
  {
    compactionResource.updateClusterConfig(clusterConfig);
    LOG.info("Updated cluster config to [%s]", clusterConfig);
  }

  private void submitCompactionConfig(
      Integer maxRowsPerSegment,
      Period skipOffsetFromLatest,
      CompactionEngine engine
  )
  {
    submitCompactionConfig(maxRowsPerSegment, skipOffsetFromLatest, null, engine);
  }

  private void submitCompactionConfig(
      Integer maxRowsPerSegment,
      Period skipOffsetFromLatest,
      UserCompactionTaskGranularityConfig granularitySpec,
      CompactionEngine engine
  )
  {
    submitCompactionConfig(maxRowsPerSegment, skipOffsetFromLatest, granularitySpec, false, engine);
  }

  private void submitCompactionConfig(
      Integer maxRowsPerSegment,
      Period skipOffsetFromLatest,
      UserCompactionTaskGranularityConfig granularitySpec,
      boolean dropExisting,
      CompactionEngine engine
  )
  {
    submitCompactionConfig(
        maxRowsPerSegment,
        skipOffsetFromLatest,
        granularitySpec,
        null,
        null,
        null,
        dropExisting,
        engine
    );
  }

  private void submitCompactionConfig(
      Integer maxRowsPerSegment,
      Period skipOffsetFromLatest,
      UserCompactionTaskGranularityConfig granularitySpec,
      UserCompactionTaskDimensionsConfig dimensionsSpec,
      CompactionTransformSpec transformSpec,
      AggregatorFactory[] metricsSpec,
      boolean dropExisting,
      CompactionEngine engine
  )
  {
    submitCompactionConfig(
        new DynamicPartitionsSpec(maxRowsPerSegment, null),
        skipOffsetFromLatest,
        granularitySpec,
        dimensionsSpec,
        transformSpec,
        metricsSpec,
        dropExisting,
        engine
    );
  }

  private void submitCompactionConfig(
      PartitionsSpec partitionsSpec,
      Period skipOffsetFromLatest,
      UserCompactionTaskGranularityConfig granularitySpec,
      UserCompactionTaskDimensionsConfig dimensionsSpec,
      CompactionTransformSpec transformSpec,
      AggregatorFactory[] metricsSpec,
      boolean dropExisting,
      CompactionEngine engine
  )
  {
    DataSourceCompactionConfig dataSourceCompactionConfig =
        InlineSchemaDataSourceCompactionConfig.builder()
                                              .forDataSource(fullDatasourceName)
                                              .withSkipOffsetFromLatest(skipOffsetFromLatest)
                                              .withTuningConfig(
                                            new UserCompactionTaskQueryTuningConfig(
                                                null,
                                                null,
                                                null,
                                                null,
                                                new MaxSizeSplitHintSpec(null, 1),
                                                partitionsSpec,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                1,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                1,
                                                null
                                            )
                                        )
                                              .withGranularitySpec(granularitySpec)
                                              .withDimensionsSpec(dimensionsSpec)
                                              .withMetricsSpec(metricsSpec)
                                              .withTransformSpec(transformSpec)
                                              .withIoConfig(
                                      !dropExisting ? null : new UserCompactionTaskIOConfig(true)
                                  )
                                              .withEngine(engine)
                                              .withTaskContext(ImmutableMap.of("maxNumTasks", 2))
                                              .build();
    compactionResource.submitCompactionConfig(dataSourceCompactionConfig);

    // Verify that the compaction config is updated correctly.
    DataSourceCompactionConfig foundDataSourceCompactionConfig
        = compactionResource.getDataSourceCompactionConfig(fullDatasourceName);
    Assertions.assertNotNull(foundDataSourceCompactionConfig);
    Assertions.assertNotNull(foundDataSourceCompactionConfig.getTuningConfig());
    Assertions.assertEquals(foundDataSourceCompactionConfig.getTuningConfig().getPartitionsSpec(), partitionsSpec);
    Assertions.assertEquals(foundDataSourceCompactionConfig.getSkipOffsetFromLatest(), skipOffsetFromLatest);
  }

  private void deleteCompactionConfig()
  {
    compactionResource.deleteDataSourceCompactionConfig(fullDatasourceName);

    // Verify that the compaction config is updated correctly.
    DruidCompactionConfig compactionConfig = DruidCompactionConfig
        .empty().withDatasourceConfigs(compactionResource.getAllCompactionConfigs());
    DataSourceCompactionConfig foundDataSourceCompactionConfig
        = compactionConfig.findConfigForDatasource(fullDatasourceName).orNull();
    Assertions.assertNull(foundDataSourceCompactionConfig);
  }

  /**
   * Performs compaction of the given intervals of the test datasource,
   * {@link #fullDatasourceName}, and verifies the total number of segments in
   * the datasource after compaction.
   */
  private void forceTriggerAutoCompaction(
      List<Interval> intervals,
      boolean useSupervisors,
      int numExpectedSegmentsAfterCompaction
  ) throws Exception
  {
    if (useSupervisors) {
      // Enable compaction for the requested intervals
      final FixedIntervalOrderPolicy policy = new FixedIntervalOrderPolicy(
          intervals.stream().map(
              interval -> new FixedIntervalOrderPolicy.Candidate(fullDatasourceName, interval)
          ).collect(Collectors.toList())
      );
      updateClusterConfig(
          new ClusterCompactionConfig(0.5, intervals.size(), policy, true, null)
      );

      // Wait for scheduler to pick up the compaction job
      // Instead of sleep, we can latch on a relevant metric later
      Thread.sleep(30_000);
      waitForCompactionToFinish(numExpectedSegmentsAfterCompaction);

      // Disable all compaction
      updateClusterConfig(
          new ClusterCompactionConfig(0.5, intervals.size(), COMPACT_NOTHING_POLICY, true, null)
      );
    } else {
      forceTriggerAutoCompaction(numExpectedSegmentsAfterCompaction);
    }
  }

  private void forceTriggerAutoCompaction(int numExpectedSegmentsAfterCompaction)
  {
    compactionResource.forceTriggerAutoCompaction();
    waitForCompactionToFinish(numExpectedSegmentsAfterCompaction);
  }

  private void waitForCompactionToFinish(int numExpectedSegmentsAfterCompaction)
  {
    final Set<String> taskIds = getTaskIdsForState(dataSource);
    for (String taskId : taskIds) {
      cluster.callApi().waitForTaskToSucceed(taskId, overlord);
    }

    cluster.callApi().waitForAllSegmentsToBeAvailable(fullDatasourceName, coordinator, broker);
    verifySegmentsCount(numExpectedSegmentsAfterCompaction);
  }

  private void verifySegmentsCompacted(int expectedCompactedSegmentCount, Integer expectedMaxRowsPerSegment)
  {
    verifySegmentsCompacted(
        new DynamicPartitionsSpec(expectedMaxRowsPerSegment, Long.MAX_VALUE),
        expectedCompactedSegmentCount
    );
  }

  private void verifyTombstones(int expectedCompactedTombstoneCount)
  {
    Set<DataSegment> segments = cluster.callApi().getVisibleUsedSegments(dataSource, overlord);
    int actualTombstoneCount = 0;
    for (DataSegment segment : segments) {
      if (segment.isTombstone()) {
        actualTombstoneCount++;
      }
    }
    Assertions.assertEquals(actualTombstoneCount, expectedCompactedTombstoneCount);
  }

  private void verifySegmentsCompacted(PartitionsSpec partitionsSpec, int expectedCompactedSegmentCount)
  {
    Set<DataSegment> segments = cluster.callApi().getVisibleUsedSegments(dataSource, overlord);
    List<DataSegment> foundCompactedSegments = new ArrayList<>();
    for (DataSegment segment : segments) {
      if (segment.getLastCompactionState() != null) {
        foundCompactedSegments.add(segment);
      }
    }
    Assertions.assertEquals(foundCompactedSegments.size(), expectedCompactedSegmentCount);
    for (DataSegment compactedSegment : foundCompactedSegments) {
      Assertions.assertNotNull(compactedSegment.getLastCompactionState());
      Assertions.assertNotNull(compactedSegment.getLastCompactionState().getPartitionsSpec());
      Assertions.assertEquals(compactedSegment.getLastCompactionState().getPartitionsSpec(), partitionsSpec);
    }

  }

  private void verifySegmentsCompactedDimensionSchema(List<DimensionSchema> dimensionSchemas)
  {
    Set<DataSegment> segments = cluster.callApi().getVisibleUsedSegments(dataSource, overlord);
    List<DataSegment> foundCompactedSegments = new ArrayList<>();
    for (DataSegment segment : segments) {
      if (segment.getLastCompactionState() != null) {
        foundCompactedSegments.add(segment);
      }
    }
    for (DataSegment compactedSegment : foundCompactedSegments) {
      MatcherAssert.assertThat(
          dimensionSchemas,
          Matchers.containsInAnyOrder(
              compactedSegment.getLastCompactionState()
                              .getDimensionsSpec()
                              .getDimensions()
                              .toArray(new DimensionSchema[0]))
      );
    }
  }

  private void updateCompactionTaskSlot(double compactionTaskSlotRatio, int maxCompactionTaskSlots)
  {
    final ClusterCompactionConfig oldConfig = compactionResource.getClusterConfig();
    compactionResource.updateClusterConfig(
        new ClusterCompactionConfig(
            compactionTaskSlotRatio,
            maxCompactionTaskSlots,
            oldConfig.getCompactionPolicy(),
            oldConfig.isUseSupervisors(),
            oldConfig.getEngine()
        )
    );

    // Verify that the compaction config is updated correctly
    final ClusterCompactionConfig updatedConfig = compactionResource.getClusterConfig();
    Assertions.assertEquals(updatedConfig.getCompactionTaskSlotRatio(), compactionTaskSlotRatio);
    Assertions.assertEquals(updatedConfig.getMaxCompactionTaskSlots(), maxCompactionTaskSlots);
    LOG.info(
        "Updated compactionTaskSlotRatio[%s] and maxCompactionTaskSlots[%d]",
        compactionTaskSlotRatio, maxCompactionTaskSlots
    );
  }

  private void getAndAssertCompactionStatus(
      String fullDatasourceName,
      Matcher<Long> bytesAwaitingCompactionMatcher,
      Matcher<Long> bytesCompactedMatcher,
      Matcher<Long> bytesSkippedMatcher,
      long segmentCountAwaitingCompaction,
      long segmentCountCompacted,
      long segmentCountSkipped,
      long intervalCountAwaitingCompaction,
      long intervalCountCompacted,
      long intervalCountSkipped
  )
  {
    AutoCompactionSnapshot actualStatus = compactionResource.getCompactionStatus(fullDatasourceName);
    Assertions.assertNotNull(actualStatus);
    Assertions.assertEquals(actualStatus.getScheduleStatus(), AutoCompactionSnapshot.ScheduleStatus.RUNNING);
    MatcherAssert.assertThat(actualStatus.getBytesAwaitingCompaction(), bytesAwaitingCompactionMatcher);
    MatcherAssert.assertThat(actualStatus.getBytesCompacted(), bytesCompactedMatcher);
    MatcherAssert.assertThat(actualStatus.getBytesSkipped(), bytesSkippedMatcher);
    Assertions.assertEquals(actualStatus.getSegmentCountAwaitingCompaction(), segmentCountAwaitingCompaction);
    Assertions.assertEquals(actualStatus.getSegmentCountCompacted(), segmentCountCompacted);
    Assertions.assertEquals(actualStatus.getSegmentCountSkipped(), segmentCountSkipped);
    Assertions.assertEquals(actualStatus.getIntervalCountAwaitingCompaction(), intervalCountAwaitingCompaction);
    Assertions.assertEquals(actualStatus.getIntervalCountCompacted(), intervalCountCompacted);
    Assertions.assertEquals(actualStatus.getIntervalCountSkipped(), intervalCountSkipped);
  }
  
  private List<TaskStatusPlus> getCompleteTasksForDataSource(String dataSource)
  {
    return ImmutableList.copyOf(
        (CloseableIterator<TaskStatusPlus>) cluster.callApi().onLeaderOverlord(
            o -> o.taskStatuses("complete", dataSource, 100)
        )
    );
  }

  private TaskPayloadResponse getTaskPayload(String taskId)
  {
    return cluster.callApi().onLeaderOverlord(o -> o.taskPayload(taskId));
  }

  private Set<String> getTaskIdsForState(String dataSource)
  {
    return ImmutableList.copyOf(
        (CloseableIterator<TaskStatusPlus>) cluster.callApi().onLeaderOverlord(o -> o.taskStatuses(null, dataSource, 0))
    ).stream().map(TaskStatusPlus::getId).collect(Collectors.toSet());
  }
}
