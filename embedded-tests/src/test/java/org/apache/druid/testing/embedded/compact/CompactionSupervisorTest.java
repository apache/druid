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
import org.apache.druid.catalog.guice.CatalogClientModule;
import org.apache.druid.catalog.guice.CatalogCoordinatorModule;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.indexer.partitions.DimensionRangePartitionsSpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.indexing.common.task.TaskBuilder;
import org.apache.druid.indexing.compact.CascadingReindexingTemplate;
import org.apache.druid.indexing.compact.CompactionSupervisorSpec;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.http.ClientSqlQuery;
import org.apache.druid.rpc.UpdateResponse;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.metadata.DefaultIndexingStateFingerprintMapper;
import org.apache.druid.segment.metadata.IndexingStateCache;
import org.apache.druid.segment.metadata.IndexingStateFingerprintMapper;
import org.apache.druid.segment.transform.CompactionTransformSpec;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.server.compaction.InlineReindexingRuleProvider;
import org.apache.druid.server.compaction.ReindexingDeletionRule;
import org.apache.druid.server.compaction.ReindexingIOConfigRule;
import org.apache.druid.server.compaction.ReindexingSegmentGranularityRule;
import org.apache.druid.server.compaction.ReindexingTuningConfigRule;
import org.apache.druid.server.coordinator.ClusterCompactionConfig;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.InlineSchemaDataSourceCompactionConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskGranularityConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskIOConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskQueryTuningConfig;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.indexing.MoreResources;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Embedded test that runs compaction supervisors of various types.
 */
public class CompactionSupervisorTest extends EmbeddedClusterTestBase
{
  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer()
      .setServerMemory(2_000_000_000L)
      .addProperty("druid.worker.capacity", "20");
  private final EmbeddedOverlord overlord = new EmbeddedOverlord()
      .addProperty("druid.manager.segments.pollDuration", "PT1s")
      .addProperty("druid.manager.segments.useIncrementalCache", "always");
  private final EmbeddedHistorical historical = new EmbeddedHistorical();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator()
      .addProperty("druid.manager.segments.useIncrementalCache", "always");

  @Override
  public EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .useLatchableEmitter()
                               .useDefaultTimeoutForLatchableEmitter(600)
                               .addCommonProperty("druid.auth.authorizers", "[\"allowAll\"]")
                               .addCommonProperty("druid.auth.authorizer.allowAll.type", "allowAll")
                               .addCommonProperty("druid.auth.authorizer.allowAll.policy.type", "noRestriction")
                               .addCommonProperty(
                                   "druid.policy.enforcer.allowedPolicies",
                                   "[\"org.apache.druid.query.policy.NoRestrictionPolicy\"]"
                               )
                               .addCommonProperty("druid.policy.enforcer.type", "restrictAllTables")
                               .addExtensions(CatalogClientModule.class, CatalogCoordinatorModule.class)
                               .addServer(coordinator)
                               .addServer(overlord)
                               .addServer(indexer)
                               .addServer(historical)
                               .addServer(broker)
                               .addServer(new EmbeddedRouter());
  }


  private void configureCompaction(CompactionEngine compactionEngine)
  {
    final UpdateResponse updateResponse = cluster.callApi().onLeaderOverlord(
        o -> o.updateClusterCompactionConfig(new ClusterCompactionConfig(1.0, 100, null, true, compactionEngine, true))
    );
    Assertions.assertTrue(updateResponse.isSuccess());
  }

  @MethodSource("getEngine")
  @ParameterizedTest(name = "compactionEngine={0}")
  public void test_ingestDayGranularity_andCompactToMonthGranularity_andCompactToYearGranularity_withInlineConfig(CompactionEngine compactionEngine)
  {
    configureCompaction(compactionEngine);

    // Ingest data at DAY granularity and verify
    runIngestionAtGranularity(
        "DAY",
        "2025-06-01T00:00:00.000Z,shirt,105"
        + "\n2025-06-02T00:00:00.000Z,trousers,210"
        + "\n2025-06-03T00:00:00.000Z,jeans,150"
    );
    Assertions.assertEquals(3, getNumSegmentsWith(Granularities.DAY));

    // Create a compaction config with MONTH granularity
    InlineSchemaDataSourceCompactionConfig monthGranularityConfig =
        InlineSchemaDataSourceCompactionConfig
            .builder()
            .forDataSource(dataSource)
            .withSkipOffsetFromLatest(Period.seconds(0))
            .withGranularitySpec(
                new UserCompactionTaskGranularityConfig(Granularities.MONTH, null, null)
            )
            .withTuningConfig(
                createTuningConfigWithPartitionsSpec(
                    new DimensionRangePartitionsSpec(null, 5000, List.of("item"), false)
                )
            )
            .build();

    runCompactionWithSpec(monthGranularityConfig);
    waitForAllCompactionTasksToFinish();

    Assertions.assertEquals(0, getNumSegmentsWith(Granularities.DAY));
    Assertions.assertEquals(1, getNumSegmentsWith(Granularities.MONTH));

    verifyCompactedSegmentsHaveFingerprints(monthGranularityConfig);

    InlineSchemaDataSourceCompactionConfig yearGranConfig =
        InlineSchemaDataSourceCompactionConfig
            .builder()
            .forDataSource(dataSource)
            .withSkipOffsetFromLatest(Period.seconds(0))
            .withGranularitySpec(
                new UserCompactionTaskGranularityConfig(Granularities.YEAR, null, null)
            )
            .withTuningConfig(
                createTuningConfigWithPartitionsSpec(
                    new DimensionRangePartitionsSpec(null, 5000, List.of("item"), false)
                )
            )
            .build();

    overlord.latchableEmitter().flush(); // flush events so wait for works correctly on the next round of compaction
    runCompactionWithSpec(yearGranConfig);
    waitForAllCompactionTasksToFinish();

    Assertions.assertEquals(0, getNumSegmentsWith(Granularities.DAY));
    Assertions.assertEquals(0, getNumSegmentsWith(Granularities.MONTH));
    Assertions.assertEquals(1, getNumSegmentsWith(Granularities.YEAR));

    verifyCompactedSegmentsHaveFingerprints(yearGranConfig);
  }

  @MethodSource("getEngine")
  @ParameterizedTest(name = "compactionEngine={0}")
  public void test_compaction_withPersistLastCompactionStateFalse_storesOnlyFingerprint(CompactionEngine compactionEngine)
  {
    // Configure cluster with storeCompactionStatePerSegment=false
    final UpdateResponse updateResponse = cluster.callApi().onLeaderOverlord(
        o -> o.updateClusterCompactionConfig(
            new ClusterCompactionConfig(1.0, 100, null, true, compactionEngine, false)
        )
    );
    Assertions.assertTrue(updateResponse.isSuccess());

    // Ingest data at DAY granularity
    runIngestionAtGranularity(
        "DAY",
        "2025-06-01T00:00:00.000Z,shirt,105\n"
        + "2025-06-02T00:00:00.000Z,trousers,210"
    );
    Assertions.assertEquals(2, getNumSegmentsWith(Granularities.DAY));

    // Create compaction config to compact to MONTH granularity
    InlineSchemaDataSourceCompactionConfig monthConfig =
        InlineSchemaDataSourceCompactionConfig
            .builder()
            .forDataSource(dataSource)
            .withSkipOffsetFromLatest(Period.seconds(0))
            .withGranularitySpec(
                new UserCompactionTaskGranularityConfig(Granularities.MONTH, null, null)
            )
            .withTuningConfig(
                createTuningConfigWithPartitionsSpec(
                    new DimensionRangePartitionsSpec(1000, null, List.of("item"), false)
                )
            )
            .build();

    runCompactionWithSpec(monthConfig);
    waitForAllCompactionTasksToFinish();

    verifySegmentsHaveNullLastCompactionStateAndNonNullFingerprint();
  }

  @Test
  public void test_cascadingCompactionTemplate_multiplePeriodsApplyDifferentCompactionRules()
  {
    // We eventually want to run with parameterized test for both engines but right now using RANGE partitioning and filtering
    // out all rows with native engine cant handle right now.
    CompactionEngine compactionEngine = CompactionEngine.MSQ;
    configureCompaction(compactionEngine);

    DateTime now = DateTimes.nowUtc();

    // Note that we are purposely creating events in intervals like this to make the test deterministic regardless of when it is run.
    // The supervisor will use the current time as reference time to determine which rules apply to which segments so we take extra
    // care to create segments that fall cleanly into the different rule periods that we are testing.
    String freshEvents = generateEventsInInterval(
        new Interval(now.minusHours(4), now),
        4,
        Duration.ofMinutes(30).toMillis()
    );
    String hourRuleEvents = generateEventsInInterval(
        new Interval(now.minusDays(3), now.minusDays(2)),
        5,
        Duration.ofMinutes(90).toMillis()
    );
    String dayRuleEvents = generateEventsInInterval(
        new Interval(now.minusDays(31), now.minusDays(14)),
        7,
        Duration.ofHours(25).toMillis()
    );

    String allData = freshEvents + "\n" + hourRuleEvents + "\n" + dayRuleEvents;

    runIngestionAtGranularity(
        "FIFTEEN_MINUTE",
        allData
    );
    Assertions.assertEquals(16, getNumSegmentsWith(Granularities.FIFTEEN_MINUTE));

    ReindexingSegmentGranularityRule hourRule = new ReindexingSegmentGranularityRule(
        "hourRule",
        "Compact to HOUR granularity for data older than 1 days",
        Period.days(1),
        Granularities.HOUR
    );
    ReindexingSegmentGranularityRule dayRule = new ReindexingSegmentGranularityRule(
        "dayRule",
        "Compact to DAY granularity for data older than 7 days",
        Period.days(7),
        Granularities.DAY
    );

    ReindexingTuningConfigRule tuningConfigRule = new ReindexingTuningConfigRule(
        "tuningConfigRule",
        "Use dimension range partitioning with max 1000 rows per segment",
        Period.days(1),
        createTuningConfigWithPartitionsSpec(new DimensionRangePartitionsSpec(1000, null, List.of("item"), false))
    );

    ReindexingDeletionRule deletionRule = new ReindexingDeletionRule(
        "deletionRule",
        "Drop rows where item is 'hat'",
        Period.days(7),
        new SelectorDimFilter("item", "hat", null),
        null
    );

    InlineReindexingRuleProvider.Builder ruleProvider = InlineReindexingRuleProvider.builder()
                                                                            .segmentGranularityRules(List.of(hourRule, dayRule))
                                                                            .tuningConfigRules(List.of(tuningConfigRule))
                                                                            .deletionRules(List.of(deletionRule));

    if (compactionEngine == CompactionEngine.NATIVE) {
      ruleProvider = ruleProvider.ioConfigRules(
          List.of(new ReindexingIOConfigRule("dropExisting", null, Period.days(7), new UserCompactionTaskIOConfig(true)))
      );
    }

    CascadingReindexingTemplate cascadingReindexingTemplate = new CascadingReindexingTemplate(
        dataSource,
        null,
        null,
        ruleProvider.build(),
        compactionEngine,
        null,
        null,
        null,
        Granularities.DAY
    );
    runCompactionWithSpec(cascadingReindexingTemplate);
    waitForAllCompactionTasksToFinish();

    Assertions.assertEquals(4, getNumSegmentsWith(Granularities.FIFTEEN_MINUTE));
    Assertions.assertEquals(5, getNumSegmentsWith(Granularities.HOUR));
    Assertions.assertEquals(7, getNumSegmentsWith(Granularities.DAY));
    verifyEventCountOlderThan(Period.days(7), "item", "hat", 0);
  }

  @Test
  public void test_cascadingReindexing_withVirtualColumnOnNestedData_filtersCorrectly()
  {
    // Virtual Collumns on nested data is only supported with MSQ compaction engine right now.
    CompactionEngine compactionEngine = CompactionEngine.MSQ;
    configureCompaction(compactionEngine);

    String jsonDataWithNestedColumn =
        "{\"timestamp\":\"2025-06-01T00:00:00.000Z\",\"item\":\"shirt\",\"value\":105,"
        + "\"extraInfo\":{\"fieldA\":\"valueA\",\"fieldB\":\"valueB\"}}\n"
        + "{\"timestamp\":\"2025-06-02T00:00:00.000Z\",\"item\":\"trousers\",\"value\":210,"
        + "\"extraInfo\":{\"fieldA\":\"valueC\",\"fieldB\":\"valueD\"}}\n"
        + "{\"timestamp\":\"2025-06-03T00:00:00.000Z\",\"item\":\"jeans\",\"value\":150,"
        + "\"extraInfo\":{\"fieldA\":\"valueA\",\"fieldB\":\"valueE\"}}\n"
        + "{\"timestamp\":\"2025-06-04T00:00:00.000Z\",\"item\":\"hat\",\"value\":50,"
        + "\"extraInfo\":{\"fieldA\":\"valueF\",\"fieldB\":\"valueG\"}}";

    final TaskBuilder.Index task = TaskBuilder
        .ofTypeIndex()
        .dataSource(dataSource)
        .jsonInputFormat()
        .inlineInputSourceWithData(jsonDataWithNestedColumn)
        .isoTimestampColumn("timestamp")
        .schemaDiscovery()
        .segmentGranularity("DAY");

    cluster.callApi().runTask(task.withId(IdUtils.getRandomId()), overlord);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);

    Assertions.assertEquals(4, getTotalRowCount());

    VirtualColumns virtualColumns = VirtualColumns.create(
        ImmutableList.of(
            new ExpressionVirtualColumn(
                "extractedFieldA",
                "json_value(extraInfo, '$.fieldA')",
                ColumnType.STRING,
                TestExprMacroTable.INSTANCE
            )
        )
    );

    ReindexingDeletionRule deletionRule = new ReindexingDeletionRule(
        "deleteByNestedField",
        "Remove rows where extraInfo.fieldA = 'valueA'",
        Period.days(7),
        new SelectorDimFilter("extractedFieldA", "valueA", null),
        virtualColumns
    );

    ReindexingTuningConfigRule tuningConfigRule = new ReindexingTuningConfigRule(
        "tuningConfigRule",
        null,
        Period.days(7),
        createTuningConfigWithPartitionsSpec(new DynamicPartitionsSpec(null, null))
    );

    CascadingReindexingTemplate cascadingTemplate = new CascadingReindexingTemplate(
        dataSource,
        null,
        null,
        InlineReindexingRuleProvider.builder()
                                    .deletionRules(List.of(deletionRule))
                                    .tuningConfigRules(List.of(tuningConfigRule))
                                    .build(),
        compactionEngine,
        null,
        null,
        null,
        Granularities.DAY
    );

    runCompactionWithSpec(cascadingTemplate);
    waitForAllCompactionTasksToFinish();

    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);

    // Verify: Should have 2 rows left (valueA appeared in 2 rows, both filtered out)
    Assertions.assertEquals(2, getTotalRowCount());

    // Verify the correct rows were filtered
    verifyNoRowsWithNestedValue("extraInfo", "fieldA", "valueA");
  }

  private int getTotalRowCount()
  {
    String sql = StringUtils.format("SELECT COUNT(*) as cnt FROM \"%s\"", dataSource);
    String result = cluster.callApi().onAnyBroker(b -> b.submitSqlQuery(new ClientSqlQuery(sql, null, false, false, false, null, null)));
    List<Map<String, Object>> rows = JacksonUtils.readValue(
        new DefaultObjectMapper(),
        result.getBytes(StandardCharsets.UTF_8),
        new TypeReference<>() {}
    );
    return ((Number) rows.get(0).get("cnt")).intValue();
  }

  private void verifyNoRowsWithNestedValue(String nestedColumn, String field, String value)
  {
    String sql = StringUtils.format(
        "SELECT COUNT(*) as cnt FROM \"%s\" WHERE json_value(%s, '$.%s') = '%s'",
        dataSource,
        nestedColumn,
        field,
        value
    );
    String result = cluster.callApi().onAnyBroker(b -> b.submitSqlQuery(new ClientSqlQuery(sql, null, false, false, false, null, null)));
    List<Map<String, Object>> rows = JacksonUtils.readValue(
        new DefaultObjectMapper(),
        result.getBytes(StandardCharsets.UTF_8),
        new TypeReference<>() {}
    );
    Assertions.assertEquals(
        0,
        ((Number) rows.get(0).get("cnt")).intValue(),
        StringUtils.format("Expected no rows where %s.%s = '%s'", nestedColumn, field, value)
    );
  }


  private String generateEventsInInterval(Interval interval, int numEvents, long spacingMillis)
  {
    List<String> events = new ArrayList<>();

    for (int i = 1; i <= numEvents; i++) {
      DateTime eventTime = interval.getStart().plus(spacingMillis * i);
      if (eventTime.isAfter(interval.getEnd())) {
        throw new IAE("Interval cannot fit [%d] events with spacing of [%d] millis", numEvents, spacingMillis);
      }
      String item = i % 2 == 0 ? "hat" : "shirt";
      int metricValue = 100 + i * 5;
      events.add(eventTime + "," + item + "," + metricValue);
    }

    return String.join("\n", events);
  }


  /**
   * Tests that when a compaction task filters out all rows using a transform spec,
   * tombstones are created to properly drop the old segments. This test covers both
   * hash and range partitioning strategies.
   *
   * This regression test addresses a bug where compaction with transforms that filter
   * all rows would succeed but not create tombstones, leaving old segments visible
   * and causing indefinite compaction retries.
   */
  @MethodSource("getEngineAndPartitionType")
  @ParameterizedTest(name = "compactionEngine={0}, partitionType={1}")
  public void test_compactionWithTransformFilteringAllRows_createsTombstones(
      CompactionEngine compactionEngine,
      String partitionType
  )
  {
    configureCompaction(compactionEngine);

    runIngestionAtGranularity(
        "DAY",
        "2025-06-01T00:00:00.000Z,hat,105"
        + "\n2025-06-02T00:00:00.000Z,shirt,210"
        + "\n2025-06-03T00:00:00.000Z,shirt,150"
    );

    int initialSegmentCount = getNumSegmentsWith(Granularities.DAY);
    Assertions.assertEquals(3, initialSegmentCount, "Should have 3 initial segments");

    InlineSchemaDataSourceCompactionConfig.Builder builder = InlineSchemaDataSourceCompactionConfig
        .builder()
        .forDataSource(dataSource)
        .withSkipOffsetFromLatest(Period.seconds(0))
        .withGranularitySpec(
            new UserCompactionTaskGranularityConfig(Granularities.DAY, null, null)
        )
        .withTransformSpec(
            // This filter drops all rows: expression "false" always evaluates to false
            new CompactionTransformSpec(
                new NotDimFilter(new SelectorDimFilter("item", "shirt", null))
            )
        );

    if (compactionEngine == CompactionEngine.NATIVE) {
      builder = builder.withIoConfig(
          // Enable REPLACE mode to create tombstones when no segments are produced
          new UserCompactionTaskIOConfig(true)
      );
    }

    // Add partitioning spec based on test parameter
    if ("range".equals(partitionType)) {
      builder.withTuningConfig(
          new UserCompactionTaskQueryTuningConfig(
              null,
              null,
              null,
              null,
              null,
              new DimensionRangePartitionsSpec(null, 5000, List.of("item"), false),
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null
          )
      );
    } else {
      // Hash partitioning
      builder.withTuningConfig(
          new UserCompactionTaskQueryTuningConfig(
              null,
              null,
              null,
              null,
              null,
              new HashedPartitionsSpec(null, null, null),
              null,
              null,
              null,
              null,
              null,
              2,
              null,
              null,
              null,
              null,
              null,
              null,
              null
          )
      );
    }

    InlineSchemaDataSourceCompactionConfig compactionConfig = builder.build();

    runCompactionWithSpec(compactionConfig);
    waitForAllCompactionTasksToFinish();

    int finalSegmentCount = getNumSegmentsWith(Granularities.DAY);
    Assertions.assertEquals(
        1,
        finalSegmentCount,
        "2 of 3 segments should be dropped via tombstones when transform filters all rows where item = 'shirt'"
    );
  }

  private void verifySegmentsHaveNullLastCompactionStateAndNonNullFingerprint()
  {
    overlord
        .bindings()
        .segmentsMetadataStorage()
        .retrieveAllUsedSegments(dataSource, Segments.ONLY_VISIBLE)
        .forEach(segment -> {
          Assertions.assertNull(
              segment.getLastCompactionState(),
              "Segment " + segment.getId() + " should have null lastCompactionState"
          );
          Assertions.assertNotNull(
              segment.getIndexingStateFingerprint(),
              "Segment " + segment.getId() + " should have non-null indexingStateFingerprint"
          );
        });
  }

  private void verifyCompactedSegmentsHaveFingerprints(DataSourceCompactionConfig compactionConfig)
  {
    IndexingStateCache cache = overlord.bindings().getInstance(IndexingStateCache.class);
    IndexingStateFingerprintMapper fingerprintMapper = new DefaultIndexingStateFingerprintMapper(
        cache,
        new DefaultObjectMapper()
    );
    String expectedFingerprint = fingerprintMapper.generateFingerprint(
        dataSource,
        compactionConfig.toCompactionState()
    );

    overlord
        .bindings()
        .segmentsMetadataStorage()
        .retrieveAllUsedSegments(dataSource, Segments.ONLY_VISIBLE)
        .forEach(segment -> {
          Assertions.assertEquals(
              expectedFingerprint,
              segment.getIndexingStateFingerprint(),
              "Segment " + segment.getId() + " fingerprint should match expected fingerprint"
          );
        });
  }

  private void runCompactionWithSpec(DataSourceCompactionConfig config)
  {
    final CompactionSupervisorSpec compactionSupervisor
        = new CompactionSupervisorSpec(config, false, null);
    cluster.callApi().postSupervisor(compactionSupervisor);
  }

  private void waitForAllCompactionTasksToFinish()
  {
    // Wait for all intervals to be compacted
    overlord.latchableEmitter().waitForEvent(
        event -> event.hasMetricName("interval/waitCompact/count")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource)
                      .hasValueMatching(Matchers.equalTo(0L))
    );

    // Wait for all submitted compaction jobs to finish
    int numSubmittedTasks = overlord.latchableEmitter().getMetricValues(
        "compact/task/count",
        Map.of(DruidMetrics.DATASOURCE, dataSource)
    ).stream().mapToInt(Number::intValue).sum();

    final Matcher<Object> taskTypeMatcher = Matchers.anyOf(
        Matchers.equalTo("query_controller"),
        Matchers.equalTo("compact")
    );
    overlord.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("task/run/time")
                      .hasDimensionMatching(DruidMetrics.TASK_TYPE, taskTypeMatcher)
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource),
        agg -> agg.hasCountAtLeast(numSubmittedTasks)
    );
  }

  private int getNumSegmentsWith(Granularity granularity)
  {
    return (int) overlord
        .bindings()
        .segmentsMetadataStorage()
        .retrieveAllUsedSegments(dataSource, Segments.ONLY_VISIBLE)
        .stream()
        .filter(segment -> !segment.isTombstone())
        .filter(segment -> granularity.isAligned(segment.getInterval()))
        .count();
  }

  private void runIngestionAtGranularity(
      String granularity,
      String inlineDataCsv
  )
  {
    final IndexTask task = MoreResources.Task.BASIC_INDEX
        .get()
        .segmentGranularity(granularity)
        .inlineInputSourceWithData(inlineDataCsv)
        .dataSource(dataSource)
        .withId(IdUtils.getRandomId());
    cluster.callApi().runTask(task, overlord);
  }

  public static List<CompactionEngine> getEngine()
  {
    return List.of(CompactionEngine.NATIVE, CompactionEngine.MSQ);
  }

  /**
   * Provides test parameters for both compaction engines and partition types.
   */
  public static List<Object[]> getEngineAndPartitionType()
  {
    List<Object[]> params = new ArrayList<>();
    for (CompactionEngine engine : List.of(CompactionEngine.NATIVE, CompactionEngine.MSQ)) {
      for (String partitionType : List.of("range", "hash")) {
        if (engine == CompactionEngine.MSQ && "hash".equals(partitionType)) {
          // MSQ does not support hash partitioning in this context
          continue;
        }
        params.add(new Object[]{engine, partitionType});
      }
    }
    return params;
  }

  private void verifyEventCountOlderThan(Period period, String dimension, String value, int expectedCount)
  {
    DateTime now = DateTimes.nowUtc();
    DateTime threshold = now.minus(period);

    ClientSqlQuery query = new ClientSqlQuery(
        StringUtils.format(
            "SELECT COUNT(*) as cnt FROM \"%s\" WHERE %s = '%s' AND __time < MILLIS_TO_TIMESTAMP(%d)",
            dataSource,
            dimension,
            value,
            threshold.getMillis()
        ),
        null,
        false,
        false,
        false,
        null,
        null
    );

    final String resultAsJson = cluster.callApi().onAnyBroker(b -> b.submitSqlQuery(query));

    List<Map<String, Object>> result = JacksonUtils.readValue(
        new DefaultObjectMapper(),
        resultAsJson.getBytes(StandardCharsets.UTF_8),
        new TypeReference<>() {}
    );

    Assertions.assertEquals(1, result.size());
    Assertions.assertEquals(
        expectedCount,
        result.get(0).get("cnt"),
        StringUtils.format(
            "Expected %d events where %s='%s' older than %s",
            expectedCount,
            dimension,
            value,
            period
        )
    );
  }

  private UserCompactionTaskQueryTuningConfig createTuningConfigWithPartitionsSpec(PartitionsSpec partitionsSpec)
  {
    return new UserCompactionTaskQueryTuningConfig(
        null,
        null,
        null,
        null,
        null,
        partitionsSpec,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );
  }

}
