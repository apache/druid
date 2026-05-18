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
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.indexer.partitions.DimensionRangePartitionsSpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexing.common.task.TaskBuilder;
import org.apache.druid.indexing.compact.CascadingReindexingTemplate;
import org.apache.druid.indexing.compact.CompactionSupervisorSpec;
import org.apache.druid.indexing.compact.ReindexingTimelineView;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.http.ClientSqlQuery;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.server.compaction.InlineReindexingRuleProvider;
import org.apache.druid.server.compaction.ReindexingDeletionRule;
import org.apache.druid.server.compaction.ReindexingPartitioningRule;
import org.apache.druid.server.coordinator.InlineSchemaDataSourceCompactionConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskGranularityConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskQueryTuningConfig;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Embedded test that runs cascading reindexing supervisors and exercises
 * the reindexing timeline HTTP endpoint.
 */
public class CascadingReindexingSupervisorTest extends CompactionSupervisorTestBase
{
  @Test
  public void test_cascadingCompactionTemplate_multiplePeriodsApplyDifferentCompactionRules()
  {
    configureCompaction(CompactionEngine.MSQ);

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

    ReindexingPartitioningRule hourRule = new ReindexingPartitioningRule(
        "hourRule",
        "Compact to HOUR granularity for data older than 1 days",
        Period.days(1),
        Granularities.HOUR,
        new DimensionRangePartitionsSpec(1000, null, List.of("item"), false),
        null
    );
    ReindexingPartitioningRule dayRule = new ReindexingPartitioningRule(
        "dayRule",
        "Compact to DAY granularity for data older than 7 days",
        Period.days(7),
        Granularities.DAY,
        new DimensionRangePartitionsSpec(1000, null, List.of("item"), false),
        null
    );

    ReindexingDeletionRule deletionRule = new ReindexingDeletionRule(
        "deletionRule",
        "Drop rows where item is 'hat'",
        Period.days(7),
        new EqualityFilter("item", ColumnType.STRING, "hat", null),
        null
    );

    InlineReindexingRuleProvider.Builder ruleProvider = InlineReindexingRuleProvider.builder()
                                                                            .partitioningRules(List.of(hourRule, dayRule))
                                                                            .deletionRules(List.of(deletionRule));

    CascadingReindexingTemplate cascadingReindexingTemplate = new CascadingReindexingTemplate(
        dataSource,
        null,
        null,
        ruleProvider.build(),
        null,
        null,
        null,
        Granularities.HOUR,
        new DimensionRangePartitionsSpec(1000, null, List.of("item"), false),
        null,
        null
    );
    runCompactionWithSpec(cascadingReindexingTemplate);
    waitForAllCompactionTasksToFinish();
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);

    Assertions.assertEquals(4, getNumSegmentsWith(Granularities.FIFTEEN_MINUTE));
    Assertions.assertEquals(5, getNumSegmentsWith(Granularities.HOUR));
    Assertions.assertEquals(4, getNumSegmentsWith(Granularities.DAY));
    verifyEventCountOlderThan(Period.days(7), "item", "hat", 0);
  }

  @Test
  public void test_cascadingReindexing_withVirtualColumnOnNestedData_filtersCorrectly()
  {
    configureCompaction(CompactionEngine.MSQ);

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
        new ExpressionVirtualColumn(
            "extractedFieldA",
            "json_value(extraInfo, '$.fieldA')",
            ColumnType.STRING,
            TestExprMacroTable.INSTANCE
        )
    );

    ReindexingDeletionRule deletionRule = new ReindexingDeletionRule(
        "deleteByNestedField",
        "Remove rows where extraInfo.fieldA = 'valueA'",
        Period.days(7),
        new EqualityFilter("extractedFieldA", ColumnType.STRING, "valueA", null),
        virtualColumns
    );

    CascadingReindexingTemplate cascadingTemplate = new CascadingReindexingTemplate(
        dataSource,
        null,
        null,
        InlineReindexingRuleProvider.builder()
                                    .deletionRules(List.of(deletionRule))
                                    .build(),
        null,
        null,
        null,
        Granularities.DAY,
        new DynamicPartitionsSpec(null, null),
        null,
        null
    );

    runCompactionWithSpec(cascadingTemplate);
    waitForAllCompactionTasksToFinish();

    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);

    // Verify: Should have 2 rows left (valueA appeared in 2 rows, both filtered out)
    Assertions.assertEquals(2, getTotalRowCount());

    // Verify the correct rows were filtered
    verifyNoRowsWithNestedValue("extraInfo", "fieldA", "valueA");
  }

  @Test
  public void test_getReindexingTimeline_returnsTimelineForCascadingSupervisor()
  {
    ReindexingPartitioningRule pratitioningRule = new ReindexingPartitioningRule(
        "dayRule",
        "Compact to DAY granularity for data older than 7 days",
        Period.days(7),
        Granularities.DAY,
        new DynamicPartitionsSpec(null, null),
        null
    );

    CascadingReindexingTemplate template = new CascadingReindexingTemplate(
        dataSource,
        null,
        null,
        InlineReindexingRuleProvider.builder()
                                    .partitioningRules(List.of(pratitioningRule))
                                    .build(),
        null,
        null,
        null,
        Granularities.HOUR,
        new DynamicPartitionsSpec(null, null),
        null,
        null
    );
    runCompactionWithSpec(template);

    String supervisorId = CompactionSupervisorSpec.getSupervisorIdForDatasource(dataSource);
    DateTime referenceTime = DateTimes.nowUtc();
    String url = StringUtils.format(
        "/druid/indexer/v1/supervisor/%s/reindexingTimeline?referenceTime=%s",
        StringUtils.urlEncode(supervisorId),
        StringUtils.urlEncode(referenceTime.toString())
    );

    ReindexingTimelineView timeline = cluster.callApi().serviceClient().onLeaderOverlord(
        mapper -> new RequestBuilder(HttpMethod.GET, url),
        new TypeReference<>() {}
    );

    Assertions.assertNotNull(timeline);
    Assertions.assertEquals(dataSource, timeline.getDataSource());
    Assertions.assertEquals(referenceTime, timeline.getReferenceTime());
    Assertions.assertNull(timeline.getValidationError(), "Timeline should have no validation errors");

    List<ReindexingTimelineView.IntervalConfig> intervals = timeline.getIntervals();
    Assertions.assertFalse(intervals.isEmpty(), "Timeline should have at least one interval");

    // Verify interval ordering: each interval's start should be before or equal to the next interval's start
    for (int i = 1; i < intervals.size(); i++) {
      Assertions.assertTrue(
          !intervals.get(i).getInterval().getStart()
                          .isBefore(intervals.get(i - 1).getInterval().getStart()),
          StringUtils.format(
              "Intervals should be ordered oldest-to-newest, but interval[%d]=%s starts before interval[%d]=%s",
              i, intervals.get(i).getInterval(),
              i - 1, intervals.get(i - 1).getInterval()
          )
      );
    }

    // Verify each interval's structural consistency and track which configured rules appear
    boolean foundPartitioningRule = false;

    for (ReindexingTimelineView.IntervalConfig intervalConfig : intervals) {
      Assertions.assertNotNull(intervalConfig.getInterval());
      Assertions.assertEquals(
          intervalConfig.getRuleCount(),
          intervalConfig.getAppliedRules().size(),
          "ruleCount should match appliedRules size for interval " + intervalConfig.getInterval()
      );

      if (intervalConfig.getRuleCount() > 0) {
        Assertions.assertNotNull(
            intervalConfig.getConfig(),
            "Interval with rules should have a non-null config for " + intervalConfig.getInterval()
        );

        for (Object rule : intervalConfig.getAppliedRules()) {
          if (rule instanceof ReindexingPartitioningRule) {
            foundPartitioningRule = true;
            ReindexingPartitioningRule partitioningRule = (ReindexingPartitioningRule) rule;
            Assertions.assertEquals("dayRule", partitioningRule.getId());
            Assertions.assertEquals(Granularities.DAY, partitioningRule.getSegmentGranularity());

            // The config for this interval should reflect DAY segment granularity
            Assertions.assertNotNull(intervalConfig.getConfig().getGranularitySpec());
            Assertions.assertEquals(
                Granularities.DAY,
                intervalConfig.getConfig().getGranularitySpec().getSegmentGranularity()
            );
          }
        }
      }
    }

    Assertions.assertTrue(foundPartitioningRule, "Timeline should contain the configured segmentGranularity rule");
  }

  @Test
  public void test_getReindexingTimeline_returns400ForNonCascadingSupervisor()
  {
    configureCompaction(CompactionEngine.MSQ);

    InlineSchemaDataSourceCompactionConfig inlineConfig =
        InlineSchemaDataSourceCompactionConfig
            .builder()
            .forDataSource(dataSource)
            .withSkipOffsetFromLatest(Period.seconds(0))
            .withGranularitySpec(
                new UserCompactionTaskGranularityConfig(Granularities.MONTH, null, null)
            )
            .withTuningConfig(
                UserCompactionTaskQueryTuningConfig.builder().partitionsSpec(new DynamicPartitionsSpec(null, null)).build()
            )
            .build();

    runCompactionWithSpec(inlineConfig);

    String supervisorId = CompactionSupervisorSpec.getSupervisorIdForDatasource(dataSource);
    String url = StringUtils.format(
        "/druid/indexer/v1/supervisor/%s/reindexingTimeline",
        StringUtils.urlEncode(supervisorId)
    );

    RuntimeException exception = Assertions.assertThrows(
        RuntimeException.class,
        () -> cluster.callApi().serviceClient().onLeaderOverlord(
            mapper -> new RequestBuilder(HttpMethod.GET, url),
            new TypeReference<ReindexingTimelineView>() {}
        )
    );
    Assertions.assertTrue(
        exception.getMessage().contains("400 Bad Request"),
        "Expected 400 Bad Request in error message but got: " + exception.getMessage()
    );
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
