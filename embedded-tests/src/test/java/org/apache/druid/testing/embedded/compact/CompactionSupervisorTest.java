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
import org.apache.druid.catalog.guice.CatalogClientModule;
import org.apache.druid.catalog.guice.CatalogCoordinatorModule;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.indexer.partitions.DimensionRangePartitionsSpec;
import org.apache.druid.indexing.common.task.IndexTask;
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
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.http.ClientSqlQuery;
import org.apache.druid.rpc.UpdateResponse;
import org.apache.druid.segment.metadata.DefaultIndexingStateFingerprintMapper;
import org.apache.druid.segment.metadata.IndexingStateCache;
import org.apache.druid.segment.metadata.IndexingStateFingerprintMapper;
import org.apache.druid.server.compaction.InlineReindexingRuleProvider;
import org.apache.druid.server.compaction.ReindexingFilterRule;
import org.apache.druid.server.compaction.ReindexingGranularityRule;
import org.apache.druid.server.compaction.ReindexingIOConfigRule;
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
                new UserCompactionTaskQueryTuningConfig(
                    null,
                    null,
                    null,
                    null,
                    null,
                    new DimensionRangePartitionsSpec(1000, null, List.of("item"), false),
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

    ReindexingGranularityRule hourRule = new ReindexingGranularityRule(
        "hourRule",
        "Compact to HOUR granularity for data older than 1 days",
        Period.days(1),
        new UserCompactionTaskGranularityConfig(Granularities.HOUR, null, false)
    );
    ReindexingGranularityRule dayRule = new ReindexingGranularityRule(
        "dayRule",
        "Compact to DAY granularity for data older than 7 days",
        Period.days(7),
        new UserCompactionTaskGranularityConfig(Granularities.DAY, null, false)
    );

    ReindexingTuningConfigRule tuningConfigRule = new ReindexingTuningConfigRule(
        "tuningConfigRule",
        "Use dimension range partitioning with max 1000 rows per segment",
        Period.days(1),
        new UserCompactionTaskQueryTuningConfig(
            null,
            null,
            null,
            null,
            null,
            new DimensionRangePartitionsSpec(1000, null, List.of("item"), false),
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

    ReindexingFilterRule filterRule = new ReindexingFilterRule(
        "filterRule",
        "Drop rows where item is 'hat'",
        Period.days(7),
        new SelectorDimFilter("item", "hat", null)
    );

    InlineReindexingRuleProvider.Builder ruleProvider = InlineReindexingRuleProvider.builder()
                                                                            .granularityRules(List.of(hourRule, dayRule))
                                                                            .tuningConfigRules(List.of(tuningConfigRule))
                                                                            .filterRules(List.of(filterRule));

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
        null
    );
    runCompactionWithSpec(cascadingReindexingTemplate);
    waitForAllCompactionTasksToFinish();

    Assertions.assertEquals(4, getNumSegmentsWith(Granularities.FIFTEEN_MINUTE));
    Assertions.assertEquals(5, getNumSegmentsWith(Granularities.HOUR));
    Assertions.assertEquals(7, getNumSegmentsWith(Granularities.DAY));
    verifyEventCountOlderThan(Period.days(7), "item", "hat", 0);
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
}
