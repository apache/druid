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
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.indexer.partitions.DimensionRangePartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.indexing.compact.CompactionSupervisorSpec;
import org.apache.druid.indexing.kafka.KafkaIndexTaskModule;
import org.apache.druid.indexing.kafka.simulate.KafkaResource;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorSpec;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorSpecBuilder;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.rpc.UpdateResponse;
import org.apache.druid.segment.metadata.DefaultIndexingStateFingerprintMapper;
import org.apache.druid.segment.metadata.IndexingStateCache;
import org.apache.druid.segment.metadata.IndexingStateFingerprintMapper;
import org.apache.druid.segment.transform.CompactionTransformSpec;
import org.apache.druid.server.compaction.CompactionCandidateSearchPolicy;
import org.apache.druid.server.compaction.MostFragmentedIntervalFirstPolicy;
import org.apache.druid.server.coordinator.ClusterCompactionConfig;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.InlineSchemaDataSourceCompactionConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskDimensionsConfig;
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
import org.apache.druid.testing.tools.EventSerializer;
import org.apache.druid.testing.tools.JsonEventSerializer;
import org.apache.druid.testing.tools.StreamGenerator;
import org.apache.druid.testing.tools.WikipediaStreamEventStreamGenerator;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.shaded.org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Embedded test that runs compaction supervisors of various types.
 */
public class CompactionSupervisorTest extends EmbeddedClusterTestBase
{
  protected final KafkaResource kafkaServer = new KafkaResource();
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
                               .addExtensions(
                                   CatalogClientModule.class,
                                   CatalogCoordinatorModule.class,
                                   KafkaIndexTaskModule.class
                               )
                               .addServer(coordinator)
                               .addServer(overlord)
                               .addServer(indexer)
                               .addServer(historical)
                               .addServer(broker)
                               .addResource(kafkaServer)
                               .addServer(new EmbeddedRouter());
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
                UserCompactionTaskQueryTuningConfig.builder()
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
                UserCompactionTaskQueryTuningConfig.builder()
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

  @Test
  public void test_incrementalCompactionWithMSQ() throws Exception
  {
    configureCompaction(
        CompactionEngine.MSQ,
        new MostFragmentedIntervalFirstPolicy(2, new HumanReadableBytes("1KiB"), null, 0.8, null)
    );
    KafkaSupervisorSpecBuilder kafkaSupervisorSpecBuilder = MoreResources.Supervisor.KAFKA_JSON
        .get()
        .withDataSchema(schema -> schema.withTimestamp(new TimestampSpec("timestamp", "iso", null))
                                        .withDimensions(DimensionsSpec.builder().useSchemaDiscovery(true).build()))
        .withTuningConfig(tuningConfig -> tuningConfig.withMaxRowsPerSegment(1))
        .withIoConfig(ioConfig -> ioConfig.withConsumerProperties(kafkaServer.consumerProperties()).withTaskCount(2));

    // Set up first topic and supervisor
    final String topic1 = IdUtils.getRandomId();
    kafkaServer.createTopicWithPartitions(topic1, 1);
    final KafkaSupervisorSpec supervisor1 = kafkaSupervisorSpecBuilder.withId(topic1).build(dataSource, topic1);
    cluster.callApi().postSupervisor(supervisor1);

    final int totalRowCount = publish1kRecords(topic1, true) + publish1kRecords(topic1, false);
    waitUntilPublishedRecordsAreIngested(totalRowCount);

    // Before compaction
    Assertions.assertEquals(4, getNumSegmentsWith(Granularities.HOUR));

    PartitionsSpec partitionsSpec = new DimensionRangePartitionsSpec(null, 5000, List.of("page"), false);
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
    Thread.sleep(2_000L);
    waitForAllCompactionTasksToFinish();

    pauseCompaction(dayGranularityConfig);
    Assertions.assertEquals(0, getNumSegmentsWith(Granularities.HOUR));
    Assertions.assertEquals(1, getNumSegmentsWith(Granularities.DAY));

    verifyCompactedSegmentsHaveFingerprints(dayGranularityConfig);

    // Set up another topic and supervisor
    final String topic2 = IdUtils.getRandomId();
    kafkaServer.createTopicWithPartitions(topic2, 1);
    final KafkaSupervisorSpec supervisor2 = kafkaSupervisorSpecBuilder.withId(topic2).build(dataSource, topic2);
    cluster.callApi().postSupervisor(supervisor2);

    // published another 1k
    final int appendedRowCount = publish1kRecords(topic2, true);
    indexer.latchableEmitter().flush();
    waitUntilPublishedRecordsAreIngested(appendedRowCount);

    Assertions.assertEquals(0, getNumSegmentsWith(Granularities.HOUR));
    // 1 compacted segment + 2 appended segment
    Assertions.assertEquals(3, getNumSegmentsWith(Granularities.DAY));

    runCompactionWithSpec(dayGranularityConfig);
    Thread.sleep(2_000L);
    waitForAllCompactionTasksToFinish();

    // performed incremental compaction
    Assertions.assertEquals(2, getNumSegmentsWith(Granularities.DAY));

    // Tear down both topics and supervisors
    kafkaServer.deleteTopic(topic1);
    cluster.callApi().postSupervisor(supervisor1.createSuspendedSpec());
    kafkaServer.deleteTopic(topic2);
    cluster.callApi().postSupervisor(supervisor2.createSuspendedSpec());
  }

  protected void waitUntilPublishedRecordsAreIngested(int expectedRowCount)
  {
    indexer.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("ingest/events/processed")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource),
        agg -> agg.hasSumAtLeast(expectedRowCount)
    );

    final int totalEventsProcessed = indexer
        .latchableEmitter()
        .getMetricValues("ingest/events/processed", Map.of(DruidMetrics.DATASOURCE, dataSource))
        .stream()
        .mapToInt(Number::intValue)
        .sum();
    Assertions.assertEquals(expectedRowCount, totalEventsProcessed);
  }

  protected int publish1kRecords(String topic, boolean useTransactions)
  {
    final EventSerializer serializer = new JsonEventSerializer(overlord.bindings().jsonMapper());
    final StreamGenerator streamGenerator = new WikipediaStreamEventStreamGenerator(serializer, 100, 100);
    List<byte[]> records = streamGenerator.generateEvents(10);

    ArrayList<ProducerRecord<byte[], byte[]>> producerRecords = new ArrayList<>();
    for (byte[] record : records) {
      producerRecords.add(new ProducerRecord<>(topic, record));
    }

    if (useTransactions) {
      kafkaServer.produceRecordsToTopic(producerRecords);
    } else {
      kafkaServer.produceRecordsWithoutTransaction(producerRecords);
    }
    return producerRecords.size();
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
                UserCompactionTaskQueryTuningConfig.builder()
                    .partitionsSpec(new DimensionRangePartitionsSpec(1000, null, List.of("item"), false))
                    .build()
            )
            .build();

    runCompactionWithSpec(monthConfig);
    waitForAllCompactionTasksToFinish();

    verifySegmentsHaveNullLastCompactionStateAndNonNullFingerprint();
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
          UserCompactionTaskQueryTuningConfig.builder()
              .partitionsSpec(new DimensionRangePartitionsSpec(null, 5000, List.of("item"), false))
              .build()
      );
    } else {
      // Hash partitioning
      builder.withTuningConfig(
          UserCompactionTaskQueryTuningConfig.builder()
              .partitionsSpec(new HashedPartitionsSpec(null, null, null))
              .maxNumConcurrentSubTasks(2)
              .build()
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
    cluster.callApi().postSupervisor(new CompactionSupervisorSpec(config, false, null));
  }

  private void pauseCompaction(DataSourceCompactionConfig config)
  {
    cluster.callApi().postSupervisor(new CompactionSupervisorSpec(config, true, null));
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
}
