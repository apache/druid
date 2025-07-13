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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexing.common.task.TaskBuilder;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.TestHelper;
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
import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class EmbeddedCompactionSparseColumnTest extends EmbeddedClusterTestBase
{
  private static final Supplier<TaskBuilder.IndexParallel> INDEX_TASK =
      () -> TaskBuilder
          .ofTypeIndexParallel()
          .jsonInputFormat()
          .isoTimestampColumn("time")
          .granularitySpec("HOUR", "HOUR", true)
          .dimensions("dimB", "dimA", "dimC", "dimD", "dimE", "dimF")
          .metricAggregates(
              new CountAggregatorFactory("ingested_events"),
              new LongSumAggregatorFactory("sum_metA", "metA")
          )
          .tuningConfig(t -> t.withPartitionsSpec(new DynamicPartitionsSpec(3, 3L)))
          .inlineInputSourceWithData(
              "{\"time\":\"2015-09-12T00:46:58.771Z\",\"dimA\":\"C\",\"dimB\":\"F\",\"metA\":1}"
              + "\n{\"time\":\"2015-09-12T00:46:58.771Z\",\"dimA\":\"C\",\"dimB\":\"J\",\"metA\":1}"
              + "\n{\"time\":\"2015-09-12T00:46:58.771Z\",\"dimA\":\"H\",\"dimB\":\"X\",\"metA\":1}"
              + "\n{\"time\":\"2015-09-12T00:46:58.771Z\",\"dimA\":\"Z\",\"dimB\":\"S\",\"metA\":1}"
              + "\n{\"time\":\"2015-09-12T00:46:58.771Z\",\"dimA\":\"H\",\"dimB\":\"X\",\"metA\":1}"
              + "\n{\"time\":\"2015-09-12T00:46:58.771Z\",\"dimA\":\"H\",\"dimB\":\"Z\",\"metA\":1}"
              + "\n{\"time\":\"2015-09-12T00:46:58.771Z\",\"dimA\":\"J\",\"dimB\":\"R\",\"metA\":1}"
              + "\n{\"time\":\"2015-09-12T00:46:58.771Z\",\"dimA\":\"H\",\"dimB\":\"T\",\"metA\":1}"
              + "\n{\"time\":\"2015-09-12T00:46:58.771Z\",\"dimA\":\"H\",\"dimB\":\"X\",\"metA\":1}"
              + "\n{\"time\":\"2015-09-12T00:46:58.771Z\",\"dimC\":\"A\",\"dimB\":\"X\",\"metA\":1}\n"
          );

  private static final Supplier<TaskBuilder.Compact> COMPACTION_TASK =
      () -> TaskBuilder
          .ofTypeCompact()
          .interval(Intervals.of("2010-10-29T05:00:00Z/2030-10-29T06:00:00Z"))
          .tuningConfig(
              t -> t.withMaxRowsInMemory(3)
                    .withMaxRowsPerSegment(3)
                    .withMaxNumConcurrentSubTasks(2)
                    .withForceGuaranteedRollup(true)
                    .withPartitionsSpec(new HashedPartitionsSpec(null, 1, null))
          );

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
                               .addServer(new EmbeddedHistorical())
                               .addServer(new EmbeddedRouter());
  }

  @Test
  public void testCompactionPerfectRollUpWithoutDimensionSpec() throws Exception
  {
    try (final Closeable ignored = unloader(dataSource)) {
      // Load and verify initial data
      loadAndVerifyDataWithSparseColumn();
      // Compaction with perfect roll up. Rolls with "X", "H" (for the first and second columns respectively) should be roll up
      runTask(COMPACTION_TASK.get().dataSource(dataSource));

      // Verify compacted data.
      // Compacted data only have one segments. First segment have the following rows:
      // The ordering of the columns will be "dimB", "dimA", "dimC", "dimD", "dimE", "dimF"
      // (This is the same as the ordering in the initial ingestion task).
      List<List<Object>> segmentRows = ImmutableList.of(
          Arrays.asList(1442016000000L, "F", "C", null, null, null, null, 1, 1),
          Arrays.asList(1442016000000L, "J", "C", null, null, null, null, 1, 1),
          Arrays.asList(1442016000000L, "R", "J", null, null, null, null, 1, 1),
          Arrays.asList(1442016000000L, "S", "Z", null, null, null, null, 1, 1),
          Arrays.asList(1442016000000L, "T", "H", null, null, null, null, 1, 1),
          Arrays.asList(1442016000000L, "X", null, "A", null, null, null, 1, 1),
          Arrays.asList(1442016000000L, "X", "H", null, null, null, null, 3, 3),
          Arrays.asList(1442016000000L, "Z", "H", null, null, null, null, 1, 1)
      );
      verifyCompactedData(segmentRows);
    }
  }

  @Test
  public void testCompactionPerfectRollUpWithLexicographicDimensionSpec() throws Exception
  {
    try (final Closeable ignored = unloader(dataSource)) {
      // Load and verify initial data
      loadAndVerifyDataWithSparseColumn();
      // Compaction with perfect roll up. Rolls with "X", "H" (for the first and second columns respectively) should be roll up
      runTask(COMPACTION_TASK.get().dataSource(dataSource).dimensions("dimA", "dimB", "dimC"));

      // Verify compacted data.
      // Compacted data only have one segments. First segment have the following rows:
      // The ordering of the columns will be "dimA", "dimB", "dimC"
      List<List<Object>> segmentRows = ImmutableList.of(
          Arrays.asList(1442016000000L, null, "X", "A", 1, 1),
          Arrays.asList(1442016000000L, "C", "F", null, 1, 1),
          Arrays.asList(1442016000000L, "C", "J", null, 1, 1),
          Arrays.asList(1442016000000L, "H", "T", null, 1, 1),
          Arrays.asList(1442016000000L, "H", "X", null, 3, 3),
          Arrays.asList(1442016000000L, "H", "Z", null, 1, 1),
          Arrays.asList(1442016000000L, "J", "R", null, 1, 1),
          Arrays.asList(1442016000000L, "Z", "S", null, 1, 1)
      );
      verifyCompactedData(segmentRows);
    }
  }

  @Test
  public void testCompactionPerfectRollUpWithNonLexicographicDimensionSpec() throws Exception
  {
    try (final Closeable ignored = unloader(dataSource)) {
      // Load and verify initial data
      loadAndVerifyDataWithSparseColumn();
      // Compaction with perfect roll up. Rolls with "X", "H" (for the first and second columns respectively) should be roll up
      runTask(COMPACTION_TASK.get().dataSource(dataSource).dimensions("dimC", "dimB", "dimA"));

      // Verify compacted data.
      // Compacted data only have one segments. First segment have the following rows:
      // The ordering of the columns will be "dimC", "dimB", "dimA"
      List<List<Object>> segment1Rows = ImmutableList.of(
          Arrays.asList(1442016000000L, null, "F", "C", 1, 1),
          Arrays.asList(1442016000000L, null, "J", "C", 1, 1),
          Arrays.asList(1442016000000L, null, "R", "J", 1, 1),
          Arrays.asList(1442016000000L, null, "S", "Z", 1, 1),
          Arrays.asList(1442016000000L, null, "T", "H", 1, 1),
          Arrays.asList(1442016000000L, null, "X", "H", 3, 3),
          Arrays.asList(1442016000000L, null, "Z", "H", 1, 1),
          Arrays.asList(1442016000000L, "A", "X", null, 1, 1)
      );
      verifyCompactedData(segment1Rows);
    }
  }

  private void loadAndVerifyDataWithSparseColumn()
  {
    runTask(INDEX_TASK.get().dataSource(dataSource));
    List<Map<String, List<List<Object>>>> expectedResultBeforeCompaction = new ArrayList<>();
    // First segments have the following rows:
    List<List<Object>> segment1Rows = ImmutableList.of(
        Arrays.asList(1442016000000L, "F", "C", null, null, null, null, 1, 1),
        Arrays.asList(1442016000000L, "J", "C", null, null, null, null, 1, 1),
        Arrays.asList(1442016000000L, "X", "H", null, null, null, null, 1, 1)
    );
    expectedResultBeforeCompaction.add(ImmutableMap.of("events", segment1Rows));
    // Second segments have the following rows:
    List<List<Object>> segment2Rows = ImmutableList.of(
        Arrays.asList(1442016000000L, "S", "Z", null, null, null, null, 1, 1),
        Arrays.asList(1442016000000L, "X", "H", null, null, null, null, 1, 1),
        Arrays.asList(1442016000000L, "Z", "H", null, null, null, null, 1, 1)
    );
    expectedResultBeforeCompaction.add(ImmutableMap.of("events", segment2Rows));
    // Third segments have the following rows:
    List<List<Object>> segment3Rows = ImmutableList.of(
        Arrays.asList(1442016000000L, "R", "J", null, null, null, null, 1, 1),
        Arrays.asList(1442016000000L, "T", "H", null, null, null, null, 1, 1),
        Arrays.asList(1442016000000L, "X", "H", null, null, null, null, 1, 1)
    );
    expectedResultBeforeCompaction.add(ImmutableMap.of("events", segment3Rows));
    // Fourth segments have the following rows:
    List<List<Object>> segment4Rows = ImmutableList.of(
        Arrays.asList(1442016000000L, "X", null, "A", null, null, null, 1, 1)
    );
    expectedResultBeforeCompaction.add(ImmutableMap.of("events", segment4Rows));
    verifyQueryResult(expectedResultBeforeCompaction, 10, 10, 1);
  }

  private void verifyCompactedData(List<List<Object>> segmentRows)
  {
    List<Map<String, List<List<Object>>>> expectedResultAfterCompaction = new ArrayList<>();
    expectedResultAfterCompaction.add(ImmutableMap.of("events", segmentRows));
    verifyQueryResult(expectedResultAfterCompaction, 8, 10, 0.8);
  }

  private void verifyQueryResult(
      List<Map<String, List<List<Object>>>> expectedScanResult,
      int expectedNumRoll,
      int expectedSumCount,
      double expectedRollupRatio
  )
  {
    Assertions.assertEquals(
        "2015-09-12T00:00:00.000Z,2015-09-12T00:00:00.000Z",
        cluster.runSql("SELECT MIN(__time), MAX(__time) FROM %s", dataSource)
    );
    Assertions.assertEquals(
        StringUtils.format("%d,%d,%s", expectedNumRoll, expectedSumCount, expectedRollupRatio),
        cluster.runSql(
            "SELECT COUNT(*) as num_rows, SUM(ingested_events) as total_events,"
            + " (COUNT(*) * 1.0) / SUM(ingested_events)"
            + " FROM %s",
            dataSource
        )
    );
    final ScanQuery scanQuery = Druids
        .newScanQueryBuilder()
        .dataSource(dataSource)
        .eternityInterval()
        .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
        .build();
    Assertions.assertEquals(toJson(expectedScanResult), getScanEvents(scanQuery));
  }

  /**
   * Runs the given scan query and extracts the "events" field from it.
   */
  private String getScanEvents(ScanQuery scanQuery)
  {
    final String resultAsJson =
        FutureUtils.getUnchecked(cluster.anyBroker().submitNativeQuery(scanQuery), true);
    final List<Map<String, Object>> resultList = JacksonUtils.readValue(
        TestHelper.JSON_MAPPER,
        resultAsJson.getBytes(StandardCharsets.UTF_8),
        new TypeReference<>() {}
    );

    final List<Map<String, Object>> trimmedResult = resultList
        .stream()
        .map(map -> Map.of("events", map.getOrDefault("events", "")))
        .collect(Collectors.toList());
    return toJson(trimmedResult);
  }

  private String toJson(Object stuff)
  {
    try {
      return TestHelper.JSON_MAPPER.writeValueAsString(stuff);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Deletes all the data for the given datasource so that compaction tasks for
   * this datasource do not take up task slots unnecessarily.
   */
  private Closeable unloader(String dataSource)
  {
    return () -> {
      overlord.bindings().segmentsMetadataStorage().markAllSegmentsAsUnused(dataSource);
    };
  }

  private void runTask(TaskBuilder<?, ?, ?> taskBuilder)
  {
    final String taskId = EmbeddedClusterApis.newTaskId(dataSource);
    cluster.callApi().onLeaderOverlord(
        o -> o.runTask(taskId, taskBuilder.withId(taskId))
    );
    cluster.callApi().waitForTaskToSucceed(taskId, overlord);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator);
  }
}
