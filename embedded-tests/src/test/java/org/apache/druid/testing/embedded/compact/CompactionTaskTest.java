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

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.druid.client.indexing.ClientCompactionTaskGranularitySpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.report.IngestionStatsAndErrors;
import org.apache.druid.indexer.report.IngestionStatsAndErrorsTaskReport;
import org.apache.druid.indexer.report.TaskReport;
import org.apache.druid.indexing.common.task.CompactionIntervalSpec;
import org.apache.druid.indexing.common.task.TaskBuilder;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchModule;
import org.apache.druid.query.aggregation.datasketches.quantiles.DoublesSketchModule;
import org.apache.druid.query.aggregation.datasketches.theta.SketchModule;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.indexing.MoreResources;
import org.apache.druid.testing.embedded.indexing.Resources;
import org.joda.time.Interval;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class CompactionTaskTest extends CompactionTestBase
{
  private static final Supplier<TaskBuilder.Index> INDEX_TASK = MoreResources.Task.INDEX_TASK_WITH_AGGREGATORS;

  private static final List<Pair<String, String>> INDEX_QUERIES_RESOURCE = List.of(
      Pair.of(Resources.Query.SELECT_MIN_MAX_TIME, "2013-08-31T01:02:33.000Z,2013-09-01T12:41:27.000Z"),
      Pair.of(Resources.Query.SELECT_APPROX_COUNT_DISTINCT, "5,5"),
      Pair.of(Resources.Query.SELECT_EARLIEST_LATEST_USER, "nuclear,stringer"),
      Pair.of(Resources.Query.SELECT_COUNT_OF_CHINESE_PAGES, "Crimson Typhoon,1,905.0,9050.0")
  );

  private static final List<Pair<String, String>> INDEX_QUERIES_YEAR_RESOURCE = List.of(
      Pair.of(Resources.Query.SELECT_MIN_MAX_TIME, "2013-01-01T00:00:00.000Z,2013-01-01T00:00:00.000Z"),
      Pair.of(Resources.Query.SELECT_APPROX_COUNT_DISTINCT, "5,5"),
      Pair.of(Resources.Query.SELECT_EARLIEST_LATEST_USER, "masterYi,speed"),
      Pair.of(Resources.Query.SELECT_COUNT_OF_CHINESE_PAGES, "Crimson Typhoon,1,1810.0,18100.0")
  );

  private static final List<Pair<String, String>> INDEX_QUERIES_HOUR_RESOURCE = List.of(
      Pair.of(Resources.Query.SELECT_MIN_MAX_TIME, "2013-08-31T01:00:00.000Z,2013-09-01T12:00:00.000Z"),
      Pair.of(Resources.Query.SELECT_APPROX_COUNT_DISTINCT, "5,5"),
      Pair.of(Resources.Query.SELECT_EARLIEST_LATEST_USER, "nuclear,stringer"),
      Pair.of(Resources.Query.SELECT_COUNT_OF_CHINESE_PAGES, "Crimson Typhoon,1,905.0,9050.0")
  );

  private static final Supplier<TaskBuilder.Compact> COMPACTION_TASK =
      () -> TaskBuilder
          .ofTypeCompact()
          .context("storeCompactionState", true)
          .ioConfig(new CompactionIntervalSpec(Intervals.of("2013-08-31/2013-09-02"), null), false);
  private static final Supplier<TaskBuilder.Compact> PARALLEL_COMPACTION_TASK =
      () -> COMPACTION_TASK.get().tuningConfig(
          t -> t.withPartitionsSpec(new HashedPartitionsSpec(null, null, null))
                .withMaxNumConcurrentSubTasks(3)
                .withForceGuaranteedRollup(true)
      );
  private static final Supplier<TaskBuilder.Compact> COMPACTION_TASK_ALLOW_NON_ALIGNED =
      () -> TaskBuilder
          .ofTypeCompact()
          .context("storeCompactionState", true)
          .ioConfig(new CompactionIntervalSpec(Intervals.of("2013-08-31/2013-09-02"), null), true);

  private static final Supplier<TaskBuilder.Index> INDEX_TASK_WITH_TIMESTAMP =
      () -> MoreResources.Task.INDEX_TASK_WITH_AGGREGATORS.get().dimensions(
          "page",
          "language", "user", "unpatrolled", "newPage", "robot", "anonymous",
          "namespace", "continent", "country", "region", "city", "timestamp"
      );

  private String fullDatasourceName;

  @BeforeEach
  public void setFullDatasourceName()
  {
    fullDatasourceName = dataSource;
  }

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .useLatchableEmitter()
                               .useDefaultTimeoutForLatchableEmitter(10)
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

  @Test
  public void testCompaction() throws Exception
  {
    loadDataAndCompact(INDEX_TASK.get(), COMPACTION_TASK.get(), null);
  }

  @Test
  public void testCompactionWithSegmentGranularity() throws Exception
  {
    loadDataAndCompact(INDEX_TASK.get(), COMPACTION_TASK_ALLOW_NON_ALIGNED.get(), Granularities.MONTH);
  }

  @Test
  public void testCompactionWithSegmentGranularityInGranularitySpec() throws Exception
  {
    loadDataAndCompact(INDEX_TASK.get(), COMPACTION_TASK_ALLOW_NON_ALIGNED.get(), Granularities.MONTH);
  }

  @Test
  public void testCompactionWithQueryGranularityInGranularitySpec() throws Exception
  {
    try (final Closeable ignored = unloader(fullDatasourceName)) {
      runTask(INDEX_TASK.get());
      // 4 segments across 2 days
      verifySegmentsCount(4);
      List<Interval> expectedIntervalAfterCompaction = getSegmentIntervals();

      verifySegmentsHaveQueryGranularity("SECOND", 4);
      verifyQuery(INDEX_QUERIES_RESOURCE);

      // QueryGranularity was SECOND, now we will change it to HOUR (QueryGranularity changed to coarser)
      compactData(COMPACTION_TASK_ALLOW_NON_ALIGNED.get(), null, Granularities.HOUR);

      // The original 4 segments should be compacted into 2 new segments since data only has 2 days and the compaction
      // segmentGranularity is DAY
      verifySegmentsCount(2);
      verifyQuery(INDEX_QUERIES_HOUR_RESOURCE);
      verifySegmentsHaveQueryGranularity("HOUR", 2);
      verifySegmentIntervals(expectedIntervalAfterCompaction);

      // QueryGranularity was HOUR, now we will change it to MINUTE (QueryGranularity changed to finer)
      compactData(COMPACTION_TASK_ALLOW_NON_ALIGNED.get(), null, Granularities.MINUTE);

      // There will be no change in number of segments as compaction segmentGranularity is the same and data interval
      // is the same. Since QueryGranularity is changed to finer qranularity, the data will remains the same. (data
      // will just be bucketed to a finer qranularity but roll up will not be different
      // i.e. 2020-10-29T05:00 will just be bucketed to 2020-10-29T05:00:00)
      verifySegmentsCount(2);
      verifyQuery(INDEX_QUERIES_HOUR_RESOURCE);
      verifySegmentsHaveQueryGranularity("MINUTE", 2);
      verifySegmentIntervals(expectedIntervalAfterCompaction);
    }
  }

  @Test
  public void testParallelHashedCompaction() throws Exception
  {
    try (final Closeable ignored = unloader(fullDatasourceName)) {
      runTask(INDEX_TASK.get());
      // 4 segments across 2 days
      verifySegmentsCount(4);
      List<Interval> expectedIntervalAfterCompaction = getSegmentIntervals();

      verifySegmentsHaveQueryGranularity("SECOND", 4);
      verifyQuery(INDEX_QUERIES_RESOURCE);
      String taskId = compactData(PARALLEL_COMPACTION_TASK.get(), null, null);

      // The original 4 segments should be compacted into 2 new segments
      verifySegmentsCount(2);
      verifyQuery(INDEX_QUERIES_RESOURCE);
      verifySegmentsHaveQueryGranularity("SECOND", 2);

      verifySegmentIntervals(expectedIntervalAfterCompaction);

      Map<String, TaskReport> reports = cluster.callApi().onLeaderOverlord(o -> o.taskReportAsMap(taskId));
      Assertions.assertTrue(reports != null && !reports.isEmpty());

      Assertions.assertEquals(
          2,
          reports.values()
                 .stream()
                 .filter(r -> r instanceof IngestionStatsAndErrorsTaskReport)
                 .mapToLong(r -> ((IngestionStatsAndErrors) r.getPayload()).getSegmentsPublished())
                 .sum()
      );
      Assertions.assertEquals(
          4,
          reports.values()
                 .stream()
                 .filter(r -> r instanceof IngestionStatsAndErrorsTaskReport)
                 .mapToLong(r -> ((IngestionStatsAndErrors) r.getPayload()).getSegmentsRead())
                 .sum()
      );
    }
  }

  @Test
  public void testCompactionWithSegmentGranularityAndQueryGranularityInGranularitySpec() throws Exception
  {
    try (final Closeable ignored = unloader(fullDatasourceName)) {
      runTask(INDEX_TASK.get());
      // 4 segments across 2 days
      verifySegmentsCount(4);
      List<Interval> expectedIntervalAfterCompaction = getSegmentIntervals();

      verifySegmentsHaveQueryGranularity("SECOND", 4);
      verifyQuery(INDEX_QUERIES_RESOURCE);
      compactData(COMPACTION_TASK_ALLOW_NON_ALIGNED.get(), Granularities.YEAR, Granularities.YEAR);

      // The original 4 segments should be compacted into 1 new segment
      verifySegmentsCount(1);
      verifyQuery(INDEX_QUERIES_YEAR_RESOURCE);
      verifySegmentsHaveQueryGranularity("YEAR", 1);

      expectedIntervalAfterCompaction = EmbeddedClusterApis.createAlignedIntervals(
          expectedIntervalAfterCompaction,
          Granularities.YEAR
      );
      verifySegmentIntervals(expectedIntervalAfterCompaction);
    }
  }

  @Test
  public void testCompactionWithTimestampDimension() throws Exception
  {
    loadDataAndCompact(INDEX_TASK_WITH_TIMESTAMP.get(), COMPACTION_TASK.get(), null);
  }

  private void loadDataAndCompact(
      TaskBuilder.Index indexTask,
      TaskBuilder.Compact compactionResource,
      Granularity newSegmentGranularity
  ) throws Exception
  {
    try (final Closeable ignored = unloader(fullDatasourceName)) {
      runTask(indexTask);
      // 4 segments across 2 days
      verifySegmentsCount(4);
      List<Interval> expectedIntervalAfterCompaction = getSegmentIntervals();

      verifySegmentsHaveQueryGranularity("SECOND", 4);
      verifyQuery(INDEX_QUERIES_RESOURCE);

      compactData(compactionResource, newSegmentGranularity, null);

      // The original 4 segments should be compacted into 2 new segments
      verifySegmentsCount(2);
      verifyQuery(INDEX_QUERIES_RESOURCE);
      verifySegmentsHaveQueryGranularity("SECOND", 2);

      if (newSegmentGranularity != null) {
        expectedIntervalAfterCompaction = EmbeddedClusterApis.createAlignedIntervals(
            expectedIntervalAfterCompaction,
            newSegmentGranularity
        );
      }
      verifySegmentIntervals(expectedIntervalAfterCompaction);
    }
  }

  private String compactData(
      TaskBuilder.Compact template,
      Granularity newSegmentGranularity,
      Granularity newQueryGranularity
  )
  {
    template.dataSource(fullDatasourceName);
    // For the new granularitySpec map
    template.granularitySpec(
        new ClientCompactionTaskGranularitySpec(newSegmentGranularity, newQueryGranularity, null)
    );
    // For the deprecated segment granularity field
    if (newSegmentGranularity != null) {
      template.segmentGranularity(newSegmentGranularity);
    }
    return runTask(template);
  }

  private void verifySegmentsHaveQueryGranularity(String expectedQueryGranularity, int segmentCount)
  {
    final SegmentMetadataQuery query = new Druids.SegmentMetadataQueryBuilder()
        .dataSource(fullDatasourceName)
        .analysisTypes(SegmentMetadataQuery.AnalysisType.QUERYGRANULARITY)
        .intervals("2013-08-31/2013-09-02")
        .build();

    final String resultAsJson = cluster.callApi().onAnyBroker(b -> b.submitNativeQuery(query));

    // Trim the result so that it contains only the `queryGranularity` fields
    final List<Map<String, Object>> resultList = JacksonUtils.readValue(
        TestHelper.JSON_MAPPER,
        resultAsJson.getBytes(StandardCharsets.UTF_8),
        new TypeReference<>() {}
    );
    final List<Map<String, Object>> trimmedResult = resultList
        .stream()
        .map(map -> Map.of("queryGranularity", map.getOrDefault("queryGranularity", "")))
        .collect(Collectors.toList());

    final List<Map<String, String>> expectedResults = new ArrayList<>();
    for (int i = 0; i < segmentCount; i++) {
      expectedResults.add(
          Map.of("queryGranularity", expectedQueryGranularity)
      );
    }

    Assertions.assertEquals(expectedResults, trimmedResult);
  }
}
