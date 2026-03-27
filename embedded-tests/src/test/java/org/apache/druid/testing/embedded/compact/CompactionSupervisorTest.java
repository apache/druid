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

import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.data.input.StringTuple;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.indexer.partitions.DimensionRangePartitionsSpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexing.common.task.TaskBuilder;
import org.apache.druid.indexing.common.task.TuningConfigBuilder;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexIOConfig;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexIngestionSpec;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexSupervisorTask;
import org.apache.druid.indexing.compact.CompactionSupervisorSpec;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.Numbers;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.rpc.UpdateResponse;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.metadata.DefaultIndexingStateFingerprintMapper;
import org.apache.druid.segment.metadata.IndexingStateCache;
import org.apache.druid.segment.metadata.IndexingStateFingerprintMapper;
import org.apache.druid.segment.transform.CompactionTransformSpec;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.server.compaction.CompactionCandidateSearchPolicy;
import org.apache.druid.server.compaction.MostFragmentedIntervalFirstPolicy;
import org.apache.druid.server.coordinator.ClusterCompactionConfig;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.InlineSchemaDataSourceCompactionConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskDimensionsConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskGranularityConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskIOConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskQueryTuningConfig;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.tools.EventSerializer;
import org.apache.druid.testing.embedded.tools.JsonEventSerializer;
import org.apache.druid.testing.embedded.tools.StreamGenerator;
import org.apache.druid.testing.embedded.tools.WikipediaStreamEventStreamGenerator;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.DimensionRangeShardSpec;
import org.hamcrest.Matchers;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Embedded test that runs compaction supervisors of various types.
 */
public class CompactionSupervisorTest extends CompactionSupervisorTestBase
{
  @MethodSource("getEngine")
  @ParameterizedTest(name = "compactionEngine={0}")
  public void test_ingestDayGranularity_andCompactToMonthGranularity_andCompactToYearGranularity_withInlineConfig(
      CompactionEngine compactionEngine
  )
  {
    configureCompaction(compactionEngine, null);

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
                UserCompactionTaskQueryTuningConfig
                    .builder()
                    .partitionsSpec(new DimensionRangePartitionsSpec(null, 5000, List.of("item"), false))
                    .build()
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
                  UserCompactionTaskQueryTuningConfig
                      .builder()
                      .partitionsSpec(new DimensionRangePartitionsSpec(null, 5000, List.of("item"), false))
                      .build()
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

  @MethodSource("getPolicyAndPartition")
  @ParameterizedTest(name = "partitionsSpec={0}")
  public void test_minorCompactionWithMSQ(MostFragmentedIntervalFirstPolicy policy, PartitionsSpec partitionsSpec)
  {
    configureCompaction(CompactionEngine.MSQ, policy);

    ingest1kRecords();
    ingest1kRecords();

    overlord.latchableEmitter().waitForNextEvent(event -> event.hasMetricName("segment/metadataCache/sync/time"));
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);
    Assertions.assertEquals(2, getNumSegmentsWith(Granularities.DAY));
    Assertions.assertEquals(2000, getTotalRowCount());

    // Create a compaction config with DAY granularity
    InlineSchemaDataSourceCompactionConfig dayGranularityConfig =
        InlineSchemaDataSourceCompactionConfig
            .builder()
            .forDataSource(dataSource)
            .withSkipOffsetFromLatest(Period.seconds(0))
            .withGranularitySpec(new UserCompactionTaskGranularityConfig(Granularities.DAY, null, false))
            .withDimensionsSpec(new UserCompactionTaskDimensionsConfig(
                WikipediaStreamEventStreamGenerator.dimensions()
                                                   .stream()
                                                   .map(StringDimensionSchema::new)
                                                   .collect(Collectors.toUnmodifiableList())))
            .withTaskContext(Map.of("useConcurrentLocks", true))
            .withIoConfig(new UserCompactionTaskIOConfig(true))
            .withTuningConfig(UserCompactionTaskQueryTuningConfig.builder().partitionsSpec(partitionsSpec).build())
            .build();

    runCompactionWithSpec(dayGranularityConfig);
    waitForAllCompactionTasksToFinish();

    pauseCompaction(dayGranularityConfig);

    overlord.latchableEmitter().waitForNextEvent(event -> event.hasMetricName("segment/metadataCache/sync/time"));
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);
    Assertions.assertEquals(1, getNumSegmentsWith(Granularities.DAY));
    Assertions.assertEquals(2000, getTotalRowCount());

    verifyCompactedSegmentsHaveFingerprints(dayGranularityConfig);

    // ingest another 2k
    ingest1kRecords();
    ingest1kRecords();

    overlord.latchableEmitter().waitForNextEvent(event -> event.hasMetricName("segment/metadataCache/sync/time"));
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);
    Assertions.assertEquals(3, getNumSegmentsWith(Granularities.DAY));
    Assertions.assertEquals(4000, getTotalRowCount());

    long totalUsed = overlord.latchableEmitter().getMetricValues(
        "segment/metadataCache/used/count",
        Map.of(DruidMetrics.DATASOURCE, dataSource)
    ).stream().reduce((first, second) -> second).orElse(0).longValue();

    runCompactionWithSpec(dayGranularityConfig);
    waitForAllCompactionTasksToFinish();

    // wait for new segments have been updated to the cache
    overlord.latchableEmitter().waitForEvent(
        event -> event.hasMetricName("segment/metadataCache/used/count")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource)
                      .hasValueMatching(Matchers.greaterThan(totalUsed)));

    // performed minor compaction: 1 previously compacted segment + 1 recently compacted segment from minor compaction
    Assertions.assertEquals(2, getNumSegmentsWith(Granularities.DAY));
    Assertions.assertEquals(4000, getTotalRowCount());
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
                  UserCompactionTaskQueryTuningConfig
                      .builder()
                      .partitionsSpec(new DimensionRangePartitionsSpec(1000, null, List.of("item"), false))
                      .build()
            )
            .build();

    runCompactionWithSpec(monthConfig);
    waitForAllCompactionTasksToFinish();

    verifySegmentsHaveNullLastCompactionStateAndNonNullFingerprint();
  }

  @Test
  public void test_compaction_cluster_by_virtualcolumn()
  {
    // Virtual Columns on nested data is only supported with MSQ compaction engine right now.
    CompactionEngine compactionEngine = CompactionEngine.MSQ;
    configureCompaction(compactionEngine, null);

    String jsonDataWithNestedColumn =
        """
            {"timestamp": "2023-01-01T00:00:00", "str":"a",    "obj":{"a": "ll"}}
            {"timestamp": "2023-01-01T00:00:00", "str":"",     "obj":{"a": "mm"}}
            {"timestamp": "2023-01-01T00:00:00", "str":"null", "obj":{"a": "nn"}}
            {"timestamp": "2023-01-01T00:00:00", "str":"b",    "obj":{"a": "oo"}}
            {"timestamp": "2023-01-01T00:00:00", "str":"c",    "obj":{"a": "pp"}}
            {"timestamp": "2023-01-01T00:00:00", "str":"d",    "obj":{"a": "qq"}}
            {"timestamp": "2023-01-01T00:00:00", "str":null,   "obj":{"a": "rr"}}
            """;

    final TaskBuilder.Index task = TaskBuilder
        .ofTypeIndex()
        .dataSource(dataSource)
        .jsonInputFormat()
        .inlineInputSourceWithData(jsonDataWithNestedColumn)
        .isoTimestampColumn("timestamp")
        .schemaDiscovery()
        .granularitySpec("DAY", null, false);

    cluster.callApi().runTask(task.withId(IdUtils.getRandomId()), overlord);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);

    Assertions.assertEquals(7, getTotalRowCount());

    VirtualColumns virtualColumns = VirtualColumns.create(
        new ExpressionVirtualColumn("v0", "json_value(obj, '$.a')", ColumnType.STRING, TestExprMacroTable.INSTANCE)
    );

    InlineSchemaDataSourceCompactionConfig config =
        InlineSchemaDataSourceCompactionConfig
            .builder()
            .forDataSource(dataSource)
            .withSkipOffsetFromLatest(Period.seconds(0))
            .withTransformSpec(
                new CompactionTransformSpec(
                    null,
                    virtualColumns
                )
            )
            .withTuningConfig(
                UserCompactionTaskQueryTuningConfig
                    .builder()
                    .partitionsSpec(new DimensionRangePartitionsSpec(4, null, List.of("v0"), false))
                    .build()
            )
            .build();

    runCompactionWithSpec(config);
    waitForAllCompactionTasksToFinish();

    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);

    List<DataSegment> segments = cluster.callApi().getVisibleUsedSegments(dataSource, overlord).stream().toList();
    Assertions.assertEquals(2, segments.size());
    Assertions.assertEquals(
        new DimensionRangeShardSpec(
            List.of("v0"),
            virtualColumns,
            null,
            StringTuple.create("oo"),
            0,
            2
        ),
        segments.get(0).getShardSpec()
    );
    Assertions.assertEquals(
        new DimensionRangeShardSpec(
            List.of("v0"),
            virtualColumns,
            StringTuple.create("oo"),
            null,
            1,
            2
        ),
        segments.get(1).getShardSpec()
    );
  }

  @Test
  public void test_compaction_cluster_by_virtualcolumn_rollup()
  {
    // Virtual Columns on nested data is only supported with MSQ compaction engine right now.
    CompactionEngine compactionEngine = CompactionEngine.MSQ;
    configureCompaction(compactionEngine, null);

    String jsonDataWithNestedColumn =
        """
            {"timestamp": "2023-01-01T00:00:00", "str":"a",    "obj":{"a": "ll"}}
            {"timestamp": "2023-01-01T00:00:00", "str":"",     "obj":{"a": "mm"}}
            {"timestamp": "2023-01-01T00:00:00", "str":"null", "obj":{"a": "nn"}}
            {"timestamp": "2023-01-01T00:00:00", "str":"b",    "obj":{"a": "oo"}}
            {"timestamp": "2023-01-01T00:00:00", "str":"c",    "obj":{"a": "pp"}}
            {"timestamp": "2023-01-01T00:00:00", "str":"d",    "obj":{"a": "qq"}}
            {"timestamp": "2023-01-01T00:00:00", "str":null,   "obj":{"a": "rr"}}
            """;

    final TaskBuilder.Index task = TaskBuilder
        .ofTypeIndex()
        .dataSource(dataSource)
        .jsonInputFormat()
        .inlineInputSourceWithData(jsonDataWithNestedColumn)
        .isoTimestampColumn("timestamp")
        .schemaDiscovery()
        .dataSchema(builder -> builder.withAggregators(new CountAggregatorFactory("count")))
        .granularitySpec("DAY", "MINUTE", true);

    cluster.callApi().runTask(task.withId(IdUtils.getRandomId()), overlord);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);

    Assertions.assertEquals(7, getTotalRowCount());

    VirtualColumns virtualColumns = VirtualColumns.create(
        new ExpressionVirtualColumn(
            "v0",
            "json_value(obj, '$.a')",
            ColumnType.STRING,
            TestExprMacroTable.INSTANCE
        )
    );

    InlineSchemaDataSourceCompactionConfig config =
        InlineSchemaDataSourceCompactionConfig
            .builder()
            .forDataSource(dataSource)
            .withSkipOffsetFromLatest(Period.seconds(0))
            .withTransformSpec(
                new CompactionTransformSpec(
                    null,
                    virtualColumns
                )
            )
            .withTuningConfig(
                UserCompactionTaskQueryTuningConfig
                    .builder()
                    .partitionsSpec(new DimensionRangePartitionsSpec(4, null, List.of("v0"), false))
                    .build()
            )
            .build();

    runCompactionWithSpec(config);
    waitForAllCompactionTasksToFinish();

    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);

    List<DataSegment> segments = cluster.callApi().getVisibleUsedSegments(dataSource, overlord).stream().toList();
    Assertions.assertEquals(2, segments.size());
    Assertions.assertEquals(
        new DimensionRangeShardSpec(
            List.of("v0"),
            virtualColumns,
            null,
            StringTuple.create("oo"),
            0,
            2
        ),
        segments.get(0).getShardSpec()
    );
    Assertions.assertEquals(
        new DimensionRangeShardSpec(
            List.of("v0"),
            virtualColumns,
            StringTuple.create("oo"),
            null,
            1,
            2
        ),
        segments.get(1).getShardSpec()
    );
  }

  /**
   * Tests that when a compaction task filters out all rows using a transform spec,
   * tombstones are created to properly drop the old segments. This test covers both
   * hash and range partitioning strategies.
   * <p>
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
    configureCompaction(compactionEngine, null);

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
                new NotDimFilter(new EqualityFilter("item", ColumnType.STRING, "shirt", null)),
                null
            )
        );

    if (compactionEngine == CompactionEngine.NATIVE) {
      builder = builder.withIoConfig(
          // Enable REPLACE mode to create tombstones when no segments are produced
          new UserCompactionTaskIOConfig(true)
      );
    }

    if ("range".equals(partitionType)) {
      builder.withTuningConfig(
          UserCompactionTaskQueryTuningConfig
              .builder()
              .partitionsSpec(new DimensionRangePartitionsSpec(null, 5000, List.of("item"), false))
              .maxNumConcurrentSubTasks(2)
              .build()
      );
    } else {
      builder.withTuningConfig(
          UserCompactionTaskQueryTuningConfig
              .builder()
              .partitionsSpec(new HashedPartitionsSpec(null, null, null))
              .maxNumConcurrentSubTasks(2)
              .build()
      );
    }

    InlineSchemaDataSourceCompactionConfig compactionConfig = builder.build();

    runCompactionWithSpec(compactionConfig);
    waitForAllCompactionTasksToFinish();
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);

    int finalSegmentCount = getNumSegmentsWith(Granularities.DAY);
    Assertions.assertEquals(
        1,
        finalSegmentCount,
        "2 of 3 segments should be dropped via tombstones when transform filters all rows where item = 'shirt'"
    );
  }

  @MethodSource("getEngine")
  @ParameterizedTest(name = "compactionEngine={0}")
  public void test_compaction_legacy_string_discovery_sparse_column(
      CompactionEngine compactionEngine
  )
  {
    // test for a bug encountered where ordering contained columns not in dimensions list
    configureCompaction(compactionEngine, null);
    String jsonallnull =
        """
            {"timestamp": "2026-03-04T00:00:00", "string":[], "another_string": "a"}
            {"timestamp": "2026-03-04T00:00:00", "string":[], "another_string": "b"}
            """;

    final TaskBuilder.Index task = TaskBuilder
        .ofTypeIndex()
        .dataSource(dataSource)
        .jsonInputFormat()
        .inlineInputSourceWithData(jsonallnull)
        .isoTimestampColumn("timestamp")
        .dataSchema(builder -> builder.withDimensions(DimensionsSpec.builder().build()))
        .segmentGranularity("DAY");

    cluster.callApi().runTask(task.withId(IdUtils.getRandomId()), overlord);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);

    List<DataSegment> segments = cluster.callApi().getVisibleUsedSegments(dataSource, overlord).stream().toList();
    Assertions.assertEquals(1, segments.size());
    Assertions.assertEquals(1, segments.get(0).getDimensions().size());

    // switch to year granularity to trigger compaction
    InlineSchemaDataSourceCompactionConfig config =
        InlineSchemaDataSourceCompactionConfig
            .builder()
            .forDataSource(dataSource)
            .withSkipOffsetFromLatest(Period.seconds(0))
            .withGranularitySpec(
                new UserCompactionTaskGranularityConfig(Granularities.YEAR, null, null)
            )
            .withTuningConfig(
                  UserCompactionTaskQueryTuningConfig
                      .builder()
                      .partitionsSpec(new DynamicPartitionsSpec(null, null))
                      .build()
            )
            .build();

    runCompactionWithSpec(config);
    waitForAllCompactionTasksToFinish();

    segments = cluster.callApi().getVisibleUsedSegments(dataSource, overlord).stream().toList();
    Assertions.assertEquals(1, segments.size());
    Assertions.assertEquals(1, segments.get(0).getDimensions().size());
  }

  private void configureCompaction(CompactionEngine compactionEngine, @Nullable CompactionCandidateSearchPolicy policy)
  {
    final UpdateResponse updateResponse = cluster.callApi().onLeaderOverlord(
        o -> o.updateClusterCompactionConfig(new ClusterCompactionConfig(
            1.0,
            100,
            policy,
            true,
            compactionEngine,
            true
        ))
    );
    Assertions.assertTrue(updateResponse.isSuccess());
  }

  protected void ingest1kRecords()
  {
    final EventSerializer serializer = new JsonEventSerializer(overlord.bindings().jsonMapper());
    final StreamGenerator streamGenerator = new WikipediaStreamEventStreamGenerator(serializer, 500, 100);
    List<byte[]> records = streamGenerator.generateEvents(2);

    final InlineInputSource input = new InlineInputSource(
        records.stream().map(b -> new String(b, StandardCharsets.UTF_8)).collect(Collectors.joining("\n")));
    final ParallelIndexIOConfig ioConfig = new ParallelIndexIOConfig(
        input,
        new JsonInputFormat(null, null, null, null, null),
        true,
        null
    );
    final ParallelIndexIngestionSpec indexIngestionSpec = new ParallelIndexIngestionSpec(
        DataSchema.builder()
                  .withDataSource(dataSource)
                  .withTimestamp(new TimestampSpec("timestamp", "iso", null))
                  .withDimensions(DimensionsSpec.builder().useSchemaDiscovery(true).build())
                  .build(),
        ioConfig,
        TuningConfigBuilder.forParallelIndexTask().build()
    );
    final String taskId = EmbeddedClusterApis.newTaskId(dataSource);
    final ParallelIndexSupervisorTask task = new ParallelIndexSupervisorTask(
        taskId,
        null,
        null,
        indexIngestionSpec,
        null
    );
    cluster.callApi().submitTask(task);
    cluster.callApi().waitForTaskToSucceed(taskId, overlord);
  }

  private int getTotalRowCount()
  {
    return Numbers.parseInt(cluster.runSql("SELECT COUNT(*) as cnt FROM \"%s\"", dataSource));
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

  private void pauseCompaction(DataSourceCompactionConfig config)
  {
    cluster.callApi().postSupervisor(new CompactionSupervisorSpec(config, true, null));
  }

  public static List<Object[]> getPolicyAndPartition()
  {
    return List.of(
        new Object[]{
            // decides minor compaction based on bytes percent
            new MostFragmentedIntervalFirstPolicy(2, new HumanReadableBytes("1KiB"), null, 80, null, null),
            new DimensionRangePartitionsSpec(null, 10_000, List.of("page"), false)
        },
        new Object[]{
            // decides minor compaction based on rows percent
            new MostFragmentedIntervalFirstPolicy(2, new HumanReadableBytes("1KiB"), null, null, 51, null),
            new DynamicPartitionsSpec(null, null)
        }
    );
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

}
