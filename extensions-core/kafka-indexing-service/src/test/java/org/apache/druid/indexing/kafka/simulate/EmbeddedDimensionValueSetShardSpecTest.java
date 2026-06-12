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

package org.apache.druid.indexing.kafka.simulate;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.granularity.UniformGranularitySpec;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.kafka.KafkaIndexTaskModule;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorSpec;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorSpecBuilder;
import org.apache.druid.indexing.seekablestream.StreamingPartitionsSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Embedded integration tests for {@link org.apache.druid.timeline.partition.DimensionValueSetShardSpec}: end-to-end
 * ingestion, publish, query correctness, and broker segment pruning feature when Kafka tasks hand off segments.
 */
public class EmbeddedDimensionValueSetShardSpecTest extends EmbeddedClusterTestBase
{
  private static final String COL_TIMESTAMP = "timestamp";
  private static final String COL_TENANT = "tenant";
  private static final String COL_VALUE = "value";

  private static final String TENANT_A = "tenant_a";
  private static final String TENANT_B = "tenant_b";

  private static final int ROWS_PER_TENANT = 10;

  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedHistorical historical = new EmbeddedHistorical();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private KafkaResource kafkaServer;

  @Override
  public EmbeddedDruidCluster createCluster()
  {
    final EmbeddedDruidCluster cluster = EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper();
    indexer.addProperty("druid.segment.handoff.pollDuration", "PT0.1s");
    // Short emission period so ingest/events/processed fires within the latch wait timeout
    cluster.addCommonProperty("druid.monitoring.emissionPeriod", "PT0.5s");
    // Defense-in-depth: keep the broker cache off so every query re-scans segments and emits
    // query/segment/time (CacheConfig already defaults all flavors to false).
    broker.addProperty("druid.broker.cache.useCache", "false");
    broker.addProperty("druid.broker.cache.populateCache", "false");
    broker.addProperty("druid.broker.cache.useResultLevelCache", "false");
    broker.addProperty("druid.broker.cache.populateResultLevelCache", "false");

    kafkaServer = new KafkaResource();

    cluster.addExtension(KafkaIndexTaskModule.class)
           .addResource(kafkaServer)
           .useLatchableEmitter()
           .addServer(coordinator)
           .addServer(overlord)
           .addServer(indexer)
           .addServer(historical)
           .addServer(broker);

    return cluster;
  }

  @Test
  public void test_twoSupervisors_withPartitionDimensionValues_segmentsPublishAndQueriesSucceed()
  {
    final String topicA = dataSource + "_topic_a";
    final String topicB = dataSource + "_topic_b";
    kafkaServer.createTopicWithPartitions(topicA, 1);
    kafkaServer.createTopicWithPartitions(topicB, 1);

    // Produce tenant-segregated records to each topic
    kafkaServer.produceRecordsToTopic(generateRecords(topicA, TENANT_A, ROWS_PER_TENANT, DateTimes.of("2025-01-01")));
    kafkaServer.produceRecordsToTopic(generateRecords(topicB, TENANT_B, ROWS_PER_TENANT, DateTimes.of("2025-01-01")));

    // Start supervisor A — observes tenant dimension from topic A
    final String supervisorIdA = dataSource + "_supe_a";
    final KafkaSupervisorSpec specA = newSupervisorBuilder()
        .withId(supervisorIdA)
        .build(dataSource, topicA);
    Assertions.assertEquals(supervisorIdA, cluster.callApi().postSupervisor(specA));

    // Start supervisor B — observes tenant dimension from topic B
    final String supervisorIdB = dataSource + "_supe_b";
    final KafkaSupervisorSpec specB = newSupervisorBuilder()
        .withId(supervisorIdB)
        .build(dataSource, topicB);
    Assertions.assertEquals(supervisorIdB, cluster.callApi().postSupervisor(specB));

    awaitRowsProcessed(ROWS_PER_TENANT * 2);

    // Realtime: all rows from both tenants are queryable.
    assertRowCounts();

    // Suspend both supervisors → handoff. (Per-task query routing is covered by DimensionValueSetShardSpecTest;
    // here both tasks share a host:port so query/node/time can't distinguish them — counts confirm correctness.)
    cluster.callApi().postSupervisor(specA.createSuspendedSpec());
    cluster.callApi().postSupervisor(specB.createSuspendedSpec());
    indexer.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("ingest/handoff/count")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource),
        agg -> agg.hasSumAtLeast(2)
    );
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);

    // Historical: same counts after publish.
    assertRowCounts();

    // Verify sys.segments: all published segments carry a dim_value_set shard spec
    verifyAllSegmentsHaveDimensionValueSetShardSpec(dataSource);

    // Verify the partitionDimensionValues contain the expected tenant values for each supervisor's segments
    final List<Map<String, Object>> shardSpecs = getShardSpecs(dataSource);
    @SuppressWarnings("unchecked")
    final Set<String> allObservedTenants = shardSpecs.stream()
        .map(spec -> ((Map<String, List<String>>) spec.get("partitionDimensionValues")).get(COL_TENANT))
        .flatMap(List::stream)
        .collect(Collectors.toSet());
    Assertions.assertTrue(allObservedTenants.contains(TENANT_A), "Expected tenant_a in partitionDimensionValues");
    Assertions.assertTrue(allObservedTenants.contains(TENANT_B), "Expected tenant_b in partitionDimensionValues");
  }

  @Test
  public void test_multiDimensionAndMultiValuePartitionDimensionValues()
  {
    final String colRegion = "region";
    final String colRegionCode = "region_code"; // numeric (Long)

    final String topicA = dataSource + "_topic_a";
    final String topicB = dataSource + "_topic_b";
    kafkaServer.createTopicWithPartitions(topicA, 1);
    kafkaServer.createTopicWithPartitions(topicB, 1);

    // Topic A: two tenants (tenant_a, tenant_x) across two regions (us-west, eu-west) with codes 1, 2
    final List<ProducerRecord<byte[], byte[]>> recordsA = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      recordsA.add(record(topicA, "%s,tenant_a,us-west,1,val_%d", DateTimes.of("2025-01-01").plusHours(i), i));
      recordsA.add(record(topicA, "%s,tenant_x,eu-west,2,val_%d", DateTimes.of("2025-01-01").plusHours(i), i + 100));
    }
    // Topic B: one tenant (tenant_b), one region (ap-south) with code 3
    final List<ProducerRecord<byte[], byte[]>> recordsB = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      recordsB.add(record(topicB, "%s,tenant_b,ap-south,3,val_%d", DateTimes.of("2025-01-01").plusHours(i), i + 200));
    }
    kafkaServer.produceRecordsToTopic(recordsA);
    kafkaServer.produceRecordsToTopic(recordsB);

    final KafkaSupervisorSpecBuilder multiDimBuilder = new KafkaSupervisorSpecBuilder()
        .withContext(Collections.singletonMap(Tasks.USE_CONCURRENT_LOCKS, true))
        .withDataSchema(
            schema -> schema
                .withTimestamp(new TimestampSpec(COL_TIMESTAMP, null, null))
                .withDimensions(
                    DimensionsSpec.builder()
                                  .setDimensions(List.of(
                                      new StringDimensionSchema(COL_TENANT),
                                      new StringDimensionSchema(colRegion),
                                      new LongDimensionSchema(colRegionCode),
                                      new StringDimensionSchema(COL_VALUE)
                                  ))
                                  .build()
                )
        )
        .withTuningConfig(
            t -> t.withMaxRowsPerSegment(1)
                  .withReleaseLocksOnHandoff(true)
                  // Track both a string dimension and a numeric dimension
                  .withStreamingPartitionsSpec(new StreamingPartitionsSpec(List.of(COL_TENANT, colRegionCode)))
        )
        .withIoConfig(
            ioConfig -> ioConfig
                .withInputFormat(new CsvInputFormat(
                    List.of(COL_TIMESTAMP, COL_TENANT, colRegion, colRegionCode, COL_VALUE),
                    null, null, false, 0, false
                ))
                .withConsumerProperties(kafkaServer.consumerProperties())
                .withTaskDuration(Period.millis(500))
                .withStartDelay(Period.millis(10))
                .withSupervisorRunPeriod(Period.millis(500))
                .withCompletionTimeout(Period.seconds(5))
                .withUseEarliestSequenceNumber(true)
        );

    cluster.callApi().postSupervisor(multiDimBuilder.withId(dataSource + "_supe_a").build(dataSource, topicA));
    cluster.callApi().postSupervisor(multiDimBuilder.withId(dataSource + "_supe_b").build(dataSource, topicB));

    awaitRowsProcessed(15); // 10 from A + 5 from B

    // Total row count
    Assertions.assertEquals("15", cluster.runSql("SELECT COUNT(*) FROM %s", dataSource));

    // String dimension — multiple values per topic
    Assertions.assertEquals("5", cluster.runSql(
        "SELECT COUNT(*) FROM %s WHERE %s = 'tenant_a'", dataSource, COL_TENANT));
    Assertions.assertEquals("5", cluster.runSql(
        "SELECT COUNT(*) FROM %s WHERE %s = 'tenant_x'", dataSource, COL_TENANT));
    Assertions.assertEquals("5", cluster.runSql(
        "SELECT COUNT(*) FROM %s WHERE %s = 'tenant_b'", dataSource, COL_TENANT));

    // Numeric dimension equality filter — region_code is a Long, getDimension() returns "1"/"2"/"3"
    // so DimensionValueSetShardSpec can prune on equality
    Assertions.assertEquals("5", cluster.runSql(
        "SELECT COUNT(*) FROM %s WHERE %s = 1", dataSource, colRegionCode));
    Assertions.assertEquals("5", cluster.runSql(
        "SELECT COUNT(*) FROM %s WHERE %s = 2", dataSource, colRegionCode));
    Assertions.assertEquals("5", cluster.runSql(
        "SELECT COUNT(*) FROM %s WHERE %s = 3", dataSource, colRegionCode));

    // Suspend, publish, and verify historical queries give the same counts.
    suspendAndAwaitHandoff(multiDimBuilder.withId(dataSource + "_supe_a").build(dataSource, topicA), 1);
    suspendAndAwaitHandoff(multiDimBuilder.withId(dataSource + "_supe_b").build(dataSource, topicB), 2);

    Assertions.assertEquals("15", cluster.runSql("SELECT COUNT(*) FROM %s", dataSource));
    Assertions.assertEquals("5", cluster.runSql(
        "SELECT COUNT(*) FROM %s WHERE %s = 1", dataSource, colRegionCode));
    Assertions.assertEquals("5", cluster.runSql(
        "SELECT COUNT(*) FROM %s WHERE %s = 'tenant_b'", dataSource, COL_TENANT));

    // Verify sys.segments: all published segments carry dim_value_set shard specs with both tracked dims
    verifyAllSegmentsHaveDimensionValueSetShardSpec(dataSource);
    final List<Map<String, Object>> shardSpecs = getShardSpecs(dataSource);
    for (Map<String, Object> spec : shardSpecs) {
      @SuppressWarnings("unchecked")
      final Map<String, List<String>> filters = (Map<String, List<String>>) spec.get("partitionDimensionValues");
      Assertions.assertTrue(
          filters.containsKey(COL_TENANT) || filters.containsKey(colRegionCode),
          "Expected at least one tracked dimension in partitionDimensionValues: " + filters
      );
    }
  }

  @Test
  public void test_incorrectValuesInTopic_inMemory_handledGracefully()
  {
    final String topic = dataSource + "_topic";
    kafkaServer.createTopicWithPartitions(topic, 1);

    // Mixed rows on a single topic — tenant_a and tenant_b both present (simulates producer bug
    // or single-topic multi-tenant setup).
    final List<ProducerRecord<byte[], byte[]>> records = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      records.add(record(topic, "%s,tenant_a,val_%d", DateTimes.of("2025-01-01").plusHours(i), i));
    }
    for (int i = 0; i < 5; i++) {
      records.add(record(topic, "%s,tenant_b,val_%d", DateTimes.of("2025-01-01").plusHours(i + 5), i + 100));
    }
    kafkaServer.produceRecordsToTopic(records);

    // Use a long task duration and large segment thresholds so nothing gets pushed to deep storage
    // while we run the in-memory correctness assertions.
    final KafkaSupervisorSpec spec = new KafkaSupervisorSpecBuilder()
        .withContext(Collections.singletonMap(Tasks.USE_CONCURRENT_LOCKS, true))
        .withDataSchema(
            schema -> schema
                .withTimestamp(new TimestampSpec(COL_TIMESTAMP, null, null))
                .withDimensions(
                    DimensionsSpec.builder()
                                  .setDimensions(List.of(
                                      new StringDimensionSchema(COL_TENANT),
                                      new StringDimensionSchema(COL_VALUE)
                                  ))
                                  .build()
                )
        )
        .withTuningConfig(
            tuningConfig -> tuningConfig
                // Large enough that 10 rows never trigger a segment push
                .withMaxRowsPerSegment(100_000)
                // Long enough that time-based persist/push doesn't fire during the test
                .withIntermediatePersistPeriod(Period.hours(1))
                .withStreamingPartitionsSpec(new StreamingPartitionsSpec(List.of(COL_TENANT)))
        )
        .withIoConfig(
            ioConfig -> ioConfig
                .withInputFormat(new CsvInputFormat(
                    List.of(COL_TIMESTAMP, COL_TENANT, COL_VALUE),
                    null, null, false, 0, false
                ))
                .withConsumerProperties(kafkaServer.consumerProperties())
                // Long task duration — task stays alive and holds data in memory
                .withTaskDuration(Period.minutes(5))
                .withStartDelay(Period.millis(10))
                .withSupervisorRunPeriod(Period.millis(500))
                .withCompletionTimeout(Period.seconds(30))
                .withUseEarliestSequenceNumber(true)
        )
        .withId(dataSource + "_supe")
        .build(dataSource, topic);

    cluster.callApi().postSupervisor(spec);
    awaitRowsProcessed(10);

    // In-memory correctness: broker fans out to the task (NumberedShardSpec, no pruning), all rows returned.
    assertMixedTenantCounts();

    suspendAndAwaitHandoff(spec, 1);

    // After publish the DimensionValueSetShardSpec carries both observed tenants; historical queries stay correct.
    assertMixedTenantCounts();

    // Verify sys.segments: published segments carry dim_value_set with both tenant values
    verifyAllSegmentsHaveDimensionValueSetShardSpec(dataSource);
    final List<Map<String, Object>> publishedShardSpecs = getShardSpecs(dataSource);
    for (Map<String, Object> shardSpec : publishedShardSpecs) {
      @SuppressWarnings("unchecked")
      final List<String> tenantValues = ((Map<String, List<String>>) shardSpec.get("partitionDimensionValues")).get(COL_TENANT);
      Assertions.assertEquals(List.of(TENANT_A, TENANT_B), tenantValues, "Expected tenant dimension in partitionDimensionValues");
    }
  }

  /**
   * One tenant per DAY segment: each segment's {@code partitionDimensionValues} must contain ONLY that segment's tenant, not
   * the union of all tenants the task saw.
   */
  @Test
  public void test_perSegmentTracking_shardSpecContainsOnlySegmentValues()
  {
    final String topic = dataSource + "_topic";
    kafkaServer.createTopicWithPartitions(topic, 1);

    final String tenantC = "tenant_c";
    final String tenantD = "tenant_d";

    // 4 tenants × 2 rows each = 8 rows, one tenant per day → 4 segments (DAY granularity).
    final List<ProducerRecord<byte[], byte[]>> records = new ArrayList<>();
    records.add(record(topic, "%s,tenant_a,val_0", DateTimes.of("2025-01-01T01:00:00")));
    records.add(record(topic, "%s,tenant_a,val_1", DateTimes.of("2025-01-01T02:00:00")));
    records.add(record(topic, "%s,tenant_b,val_2", DateTimes.of("2025-01-02T01:00:00")));
    records.add(record(topic, "%s,tenant_b,val_3", DateTimes.of("2025-01-02T02:00:00")));
    records.add(record(topic, "%s,tenant_c,val_4", DateTimes.of("2025-01-03T01:00:00")));
    records.add(record(topic, "%s,tenant_c,val_5", DateTimes.of("2025-01-03T02:00:00")));
    records.add(record(topic, "%s,tenant_d,val_6", DateTimes.of("2025-01-04T01:00:00")));
    records.add(record(topic, "%s,tenant_d,val_7", DateTimes.of("2025-01-04T02:00:00")));
    kafkaServer.produceRecordsToTopic(records);

    final KafkaSupervisorSpec spec = dayGranularitySupervisor(topic, List.of(COL_TENANT));
    cluster.callApi().postSupervisor(spec);
    awaitRowsProcessed(8);
    suspendAndAwaitHandoff(spec, 1);

    // Basic correctness: all 8 rows are queryable
    Assertions.assertEquals("8", cluster.runSql("SELECT COUNT(*) FROM %s", dataSource));
    Assertions.assertEquals("2", cluster.runSql(
        "SELECT COUNT(*) FROM %s WHERE %s = '%s'", dataSource, COL_TENANT, TENANT_A));
    Assertions.assertEquals("2", cluster.runSql(
        "SELECT COUNT(*) FROM %s WHERE %s = '%s'", dataSource, COL_TENANT, tenantC));

    // Key assertion: each segment carries ONLY the tenant value(s) observed in that segment,
    // NOT the union of all values seen by the task.
    verifyAllSegmentsHaveDimensionValueSetShardSpec(dataSource);
    final List<Map<String, Object>> shardSpecs = getShardSpecs(dataSource);
    Assertions.assertEquals(4, shardSpecs.size(),
        "Expected exactly 4 segments (DAY granularity, 4 days) but got " + shardSpecs.size());

    for (Map<String, Object> shardSpec : shardSpecs) {
      @SuppressWarnings("unchecked")
      final List<String> tenantValues =
          ((Map<String, List<String>>) shardSpec.get("partitionDimensionValues")).get(COL_TENANT);
      Assertions.assertNotNull(tenantValues, "Expected tenant in partitionDimensionValues: " + shardSpec);
      // Each segment should have exactly 1 tenant (one day = one tenant)
      Assertions.assertEquals(
          1,
          tenantValues.size(),
          StringUtils.format(
              "Expected exactly 1 tenant per segment's partitionDimensionValues but got %s",
              tenantValues
          )
      );
    }

    // Collect all tenants across all segments — the union should still cover all 4
    final Set<String> allTenants = shardSpecs.stream()
        .map(s -> {
          @SuppressWarnings("unchecked")
          final List<String> vals = ((Map<String, List<String>>) s.get("partitionDimensionValues")).get(COL_TENANT);
          return vals;
        })
        .flatMap(List::stream)
        .collect(Collectors.toSet());
    Assertions.assertEquals(
        Set.of(TENANT_A, TENANT_B, tenantC, tenantD),
        allTenants,
        "Union of all segments' partitionDimensionValues should cover all 4 tenants"
    );
  }

  @Test
  public void test_pruning_verifiedBySegmentScanMetric()
  {
    final String topic = dataSource + "_topic";
    kafkaServer.createTopicWithPartitions(topic, 1);

    // Tenant-per-DAY layout, 5 day-segments, 10 rows. Day3 repeats tenant_a (two-day match);
    // Day4 is multi-value {tenant_c,tenant_d}; Day5 is null tenant (empty CSV field).
    final List<ProducerRecord<byte[], byte[]>> records = new ArrayList<>();
    records.add(record(topic, "%s,tenant_a,val_0", DateTimes.of("2025-01-01T01:00:00")));
    records.add(record(topic, "%s,tenant_a,val_1", DateTimes.of("2025-01-01T02:00:00")));
    records.add(record(topic, "%s,tenant_b,val_2", DateTimes.of("2025-01-02T01:00:00")));
    records.add(record(topic, "%s,tenant_b,val_3", DateTimes.of("2025-01-02T02:00:00")));
    records.add(record(topic, "%s,tenant_a,val_4", DateTimes.of("2025-01-03T01:00:00")));
    records.add(record(topic, "%s,tenant_a,val_5", DateTimes.of("2025-01-03T02:00:00")));
    records.add(record(topic, "%s,tenant_c,val_6", DateTimes.of("2025-01-04T01:00:00")));
    records.add(record(topic, "%s,tenant_d,val_7", DateTimes.of("2025-01-04T02:00:00")));
    records.add(record(topic, "%s,,val_8", DateTimes.of("2025-01-05T01:00:00")));
    records.add(record(topic, "%s,,val_9", DateTimes.of("2025-01-05T02:00:00")));
    kafkaServer.produceRecordsToTopic(records);

    // DAY granularity, maxRowsPerSegment large so Day4 stays ONE multi-value segment.
    final KafkaSupervisorSpec spec = dayGranularitySupervisor(topic, List.of(COL_TENANT));
    cluster.callApi().postSupervisor(spec);
    awaitRowsProcessed(10);
    suspendAndAwaitHandoff(spec, 1); // publish + hand off all 5 segments to the historical

    // Sanity: exactly 5 published segments, all dim_value_set.
    verifyAllSegmentsHaveDimensionValueSetShardSpec(dataSource);

    // Build day(startIso) → segmentId map from sys.segments. The segment version is assigned at publish
    // time, so the expected segment ids MUST be read here and never hand-constructed.
    final Map<String, String> startToSegmentId = getStartToSegmentId(dataSource);
    Assertions.assertEquals(
        5,
        startToSegmentId.size(),
        "Expected exactly 5 day segments (1:1 start→segment) but got " + startToSegmentId
    );

    final String day1 = startToSegmentId.get("2025-01-01T00:00:00.000Z"); // tenant_a
    final String day2 = startToSegmentId.get("2025-01-02T00:00:00.000Z"); // tenant_b
    final String day3 = startToSegmentId.get("2025-01-03T00:00:00.000Z"); // tenant_a
    final String day4 = startToSegmentId.get("2025-01-04T00:00:00.000Z"); // {tenant_c,tenant_d}
    final String day5 = startToSegmentId.get("2025-01-05T00:00:00.000Z"); // null
    for (String id : List.of(day1, day2, day3, day4, day5)) {
      Assertions.assertNotNull(id, "Missing day segment id in: " + startToSegmentId);
    }

    // Positive control: no tenant filter → no pruning → all 5 scanned (proves the harness sees the full set).
    assertScan("10", Set.of(day1, day2, day3, day4, day5), "SELECT COUNT(*) FROM %s", dataSource);

    // '=' matches both tenant_a days.
    assertScan("4", Set.of(day1, day3),
               "SELECT COUNT(*) FROM %s WHERE %s = 'tenant_a'", dataSource, COL_TENANT);

    // 'IN' matches tenant_a days plus the multi-value Day4 (via tenant_c).
    assertScan("5", Set.of(day1, day3, day4),
               "SELECT COUNT(*) FROM %s WHERE %s IN ('tenant_a','tenant_c')", dataSource, COL_TENANT);

    // 'IS NULL' (domain (-inf,"")) matches only the null Day5.
    assertScan("2", Set.of(day5),
               "SELECT COUNT(*) FROM %s WHERE %s IS NULL", dataSource, COL_TENANT);

    // 'IS NOT NULL' (complement ["",+inf)) prunes the null Day5.
    assertScan("8", Set.of(day1, day2, day3, day4),
               "SELECT COUNT(*) FROM %s WHERE %s IS NOT NULL", dataSource, COL_TENANT);

    // '<>' scans everything except the tenant_a days. Scan≠count: the null Day5 is scanned (its null falls in the
    // complement (-inf,'tenant_a')) but contributes 0 rows under three-valued logic, so count=4.
    assertScan("4", Set.of(day2, day4, day5),
               "SELECT COUNT(*) FROM %s WHERE %s <> 'tenant_a'", dataSource, COL_TENANT);

    // 'NOT IN' all non-null values: only the null Day5 survives the complement; count=0 (nulls dropped under 3VL).
    assertScan("0", Set.of(day5),
               "SELECT COUNT(*) FROM %s WHERE %s NOT IN ('tenant_a','tenant_b','tenant_c','tenant_d')",
               dataSource, COL_TENANT);

    // Untracked dimension: pruning is scoped to 'tenant' (getDomainDimensions returns only it), so a 'value' filter
    // prunes nothing — all 5 scanned, filter still applied (1 row). "value" is reserved in Calcite, hence quoted.
    assertScan("1", Set.of(day1, day2, day3, day4, day5),
               "SELECT COUNT(*) FROM %s WHERE \"%s\" = 'val_0'", dataSource, COL_VALUE);

    // Non-existent value → full prune (no segment reaches the historical). Verified via the sentinel fence.
    assertFullPruneWithSentinel(
        StringUtils.format("SELECT COUNT(*) FROM %s WHERE %s = 'tenant_zzz'", dataSource, COL_TENANT),
        StringUtils.format("SELECT COUNT(*) FROM %s WHERE %s = 'tenant_b'", dataSource, COL_TENANT),
        Set.of(day2)
    );
  }

  @Test
  public void test_noPartitioning_scansAllSegments_unlikePruned()
  {
    final String topic = dataSource + "_topic";
    kafkaServer.createTopicWithPartitions(topic, 1);

    // IDENTICAL layout to test_pruning_verifiedBySegmentScanMetric: 5 day-segments, 10 rows.
    final List<ProducerRecord<byte[], byte[]>> records = new ArrayList<>();
    records.add(record(topic, "%s,tenant_a,val_0", DateTimes.of("2025-01-01T01:00:00")));
    records.add(record(topic, "%s,tenant_a,val_1", DateTimes.of("2025-01-01T02:00:00")));
    records.add(record(topic, "%s,tenant_b,val_2", DateTimes.of("2025-01-02T01:00:00")));
    records.add(record(topic, "%s,tenant_b,val_3", DateTimes.of("2025-01-02T02:00:00")));
    records.add(record(topic, "%s,tenant_a,val_4", DateTimes.of("2025-01-03T01:00:00")));
    records.add(record(topic, "%s,tenant_a,val_5", DateTimes.of("2025-01-03T02:00:00")));
    records.add(record(topic, "%s,tenant_c,val_6", DateTimes.of("2025-01-04T01:00:00")));
    records.add(record(topic, "%s,tenant_d,val_7", DateTimes.of("2025-01-04T02:00:00")));
    records.add(record(topic, "%s,,val_8", DateTimes.of("2025-01-05T01:00:00")));
    records.add(record(topic, "%s,,val_9", DateTimes.of("2025-01-05T02:00:00")));
    kafkaServer.produceRecordsToTopic(records);

    // Same layout as the partitioned test but with NO streamingPartitionsSpec → plain NumberedShardSpec, no pruning.
    final KafkaSupervisorSpec spec = dayGranularitySupervisor(topic, List.of());
    cluster.callApi().postSupervisor(spec);
    awaitRowsProcessed(10);
    suspendAndAwaitHandoff(spec, 1);

    // Partitioning is OFF: every published segment must be a plain "numbered" shard spec, NOT "dim_value_set".
    verifyAllSegmentsHaveShardSpecType(dataSource, "numbered");

    final Map<String, String> startToSegmentId = getStartToSegmentId(dataSource);
    Assertions.assertEquals(
        5,
        startToSegmentId.size(),
        "Expected exactly 5 day segments (1:1 start→segment) but got " + startToSegmentId
    );
    // Day-position → segment id, so pruned day-sets from the partitioned test can be compared across datasources.
    final String day1 = startToSegmentId.get("2025-01-01T00:00:00.000Z");
    final String day2 = startToSegmentId.get("2025-01-02T00:00:00.000Z");
    final String day3 = startToSegmentId.get("2025-01-03T00:00:00.000Z");
    final String day4 = startToSegmentId.get("2025-01-04T00:00:00.000Z");
    final String day5 = startToSegmentId.get("2025-01-05T00:00:00.000Z");
    final Set<String> allSegments = Set.of(day1, day2, day3, day4, day5);
    Assertions.assertEquals(5, allSegments.size(), "Expected 5 distinct segment ids: " + startToSegmentId);

    // Same matrix as the partitioned test; arg 3 is the partitioned twin's pruned day-set for each predicate.
    assertScansAllStrictlyMoreThanPruned("10", allSegments, Set.of(day1, day2, day3, day4, day5),
                   "SELECT COUNT(*) FROM %s", dataSource);
    assertScansAllStrictlyMoreThanPruned("4", allSegments, Set.of(day1, day3),
                   "SELECT COUNT(*) FROM %s WHERE %s = 'tenant_a'", dataSource, COL_TENANT);
    assertScansAllStrictlyMoreThanPruned("5", allSegments, Set.of(day1, day3, day4),
                   "SELECT COUNT(*) FROM %s WHERE %s IN ('tenant_a','tenant_c')", dataSource, COL_TENANT);
    assertScansAllStrictlyMoreThanPruned("2", allSegments, Set.of(day5),
                   "SELECT COUNT(*) FROM %s WHERE %s IS NULL", dataSource, COL_TENANT);
    assertScansAllStrictlyMoreThanPruned("8", allSegments, Set.of(day1, day2, day3, day4),
                   "SELECT COUNT(*) FROM %s WHERE %s IS NOT NULL", dataSource, COL_TENANT);
    assertScansAllStrictlyMoreThanPruned("4", allSegments, Set.of(day2, day4, day5),
                   "SELECT COUNT(*) FROM %s WHERE %s <> 'tenant_a'", dataSource, COL_TENANT);
    assertScansAllStrictlyMoreThanPruned("0", allSegments, Set.of(day5),
                   "SELECT COUNT(*) FROM %s WHERE %s NOT IN ('tenant_a','tenant_b','tenant_c','tenant_d')",
                   dataSource, COL_TENANT);
    // Untracked dimension: neither test prunes on 'value', so the pruned-set equals the full set. "value" is quoted
    // (reserved in Calcite).
    assertScansAllStrictlyMoreThanPruned("1", allSegments, allSegments,
                   "SELECT COUNT(*) FROM %s WHERE \"%s\" = 'val_0'", dataSource, COL_VALUE);
    // Sharpest contrast: partitioned prunes this to ZERO segments; unpartitioned still scans all 5.
    assertScansAllStrictlyMoreThanPruned("0", allSegments, Set.of(),
                   "SELECT COUNT(*) FROM %s WHERE %s = 'tenant_zzz'", dataSource, COL_TENANT);
  }

  /**
   * No-partitioning-twin assertion: asserts COUNT, then (after the {@link #assertScan} barrier) that all segments were
   * scanned, and that {@code partitionedPrunedSet} is a proper subset of the full set (or equal, for the no-prune
   * control/untracked cases) — i.e. partitioning scans strictly fewer segments.
   */
  private void assertScansAllStrictlyMoreThanPruned(
      String expectedCount,
      Set<String> expectedAll,
      Set<String> partitionedPrunedSet,
      String sqlTemplate,
      Object... sqlArgs
  )
  {
    final String sql = StringUtils.format(sqlTemplate, sqlArgs);

    historical.latchableEmitter().flush();
    Assertions.assertEquals(expectedCount, cluster.runSql(sqlTemplate, sqlArgs), "Wrong COUNT for: " + sql);
    historical.latchableEmitter().waitForEvent(
        m -> m.hasMetricName("query/time")
              .hasDimension(DruidMetrics.DATASOURCE, dataSource)
    );
    Assertions.assertEquals(
        expectedAll,
        scannedSegments(dataSource),
        "Without partitioning every predicate must scan ALL segments (no pruning) for: " + sql
    );

    // Partitioned pruned-set must be a subset of the full set, and strictly smaller for every prunable predicate.
    Assertions.assertTrue(
        expectedAll.containsAll(partitionedPrunedSet),
        "Partitioned pruned-set must be a subset of the full segment set for: " + sql
        + " pruned=" + partitionedPrunedSet + " all=" + expectedAll
    );
    if (!partitionedPrunedSet.equals(expectedAll)) {
      Assertions.assertTrue(
          partitionedPrunedSet.size() < expectedAll.size(),
          "Partitioning must scan strictly fewer segments than no-partitioning for: " + sql
          + " pruned=" + partitionedPrunedSet + " all=" + expectedAll
      );
    }
  }


  private void assertScan(String expectedCount, Set<String> expectedScanned, String sqlTemplate, Object... sqlArgs)
  {
    historical.latchableEmitter().flush();
    Assertions.assertEquals(
        expectedCount,
        cluster.runSql(sqlTemplate, sqlArgs),
        "Wrong COUNT for: " + StringUtils.format(sqlTemplate, sqlArgs)
    );
    historical.latchableEmitter().waitForEvent(
        m -> m.hasMetricName("query/time")
              .hasDimension(DruidMetrics.DATASOURCE, dataSource)
    );
    Assertions.assertEquals(
        expectedScanned,
        scannedSegments(dataSource),
        "Wrong scanned-segment set for: " + StringUtils.format(sqlTemplate, sqlArgs)
    );
  }

  private void assertFullPruneWithSentinel(String prunedSql, String sentinelSql, Set<String> sentinelExpected)
  {
    historical.latchableEmitter().flush();
    // Pruned query must return 0 and (if pruning is correct) contact zero historical segments.
    Assertions.assertEquals("0", cluster.runSql(prunedSql), "Full-prune query should return 0: " + prunedSql);
    // Sentinel: scans a deterministic, non-empty historical set; its query/time fence transitively fences any
    // (erroneous) per-segment emit produced by the pruned query above (same historical, same emitter).
    cluster.runSql(sentinelSql);
    historical.latchableEmitter().waitForEvent(
        m -> m.hasMetricName("query/time")
              .hasDimension(DruidMetrics.DATASOURCE, dataSource)
    );
    // If the pruned query wrongly scanned any segment, its id appears here as an EXTRA element → equality fails.
    Assertions.assertEquals(
        sentinelExpected,
        scannedSegments(dataSource),
        "Full-prune query must scan zero segments; only the sentinel's segments may appear. pruned=" + prunedSql
    );
  }

  /**
   * Reads {@code sys.segments} for the datasource and returns a map of segment start time (ISO) → segment id,
   * over the currently available, non-overshadowed segments. With one tenant per DAY this is a 1:1 mapping.
   */
  private Map<String, String> getStartToSegmentId(String ds)
  {
    final String csv = cluster.runSql(
        "SELECT segment_id, \"start\" FROM sys.segments"
        + " WHERE datasource = '%s' AND is_overshadowed = 0 AND is_available = 1",
        ds
    );
    final Map<String, String> startToSegmentId = new HashMap<>();
    for (String line : csv.split("\n")) {
      final String trimmed = line.trim();
      if (trimmed.isEmpty()) {
        continue;
      }
      // CSV columns: segment_id,start. The segment id uses underscores (never commas), so split into 2.
      final String[] cols = trimmed.split(",", 2);
      Assertions.assertEquals(2, cols.length, "Unexpected sys.segments CSV row: " + trimmed);
      startToSegmentId.put(cols[1].trim(), cols[0].trim());
    }
    Assertions.assertFalse(startToSegmentId.isEmpty(), "Expected at least one segment in sys.segments for " + ds);
    return startToSegmentId;
  }

  private Set<String> scannedSegments(String ds)
  {
    return historical.latchableEmitter()
        .getMetricEvents("query/segment/time")
        .stream()
        .filter(e -> ds.equals(e.getUserDims().get(DruidMetrics.DATASOURCE)))
        .map(e -> (String) e.getUserDims().get("segment"))
        .collect(Collectors.toSet());
  }


  @Test
  public void test_nullValuedDimension_isNullQueryNotPruned()
  {
    final String topic = dataSource + "_topic";
    kafkaServer.createTopicWithPartitions(topic, 1);

    final List<ProducerRecord<byte[], byte[]>> records = new ArrayList<>();
    // 5 rows with tenant_a
    for (int i = 0; i < 5; i++) {
      records.add(record(topic, "%s,tenant_a,val_%d", DateTimes.of("2025-01-01").plusHours(i), i));
    }
    // 5 rows with an empty tenant field — CsvInputFormat parses the empty column as a null dimension value
    for (int i = 0; i < 5; i++) {
      records.add(record(topic, "%s,,val_%d", DateTimes.of("2025-01-01").plusHours(i + 5), i + 100));
    }
    kafkaServer.produceRecordsToTopic(records);

    final KafkaSupervisorSpec spec = newSupervisorBuilder().withId(dataSource + "_supe").build(dataSource, topic);
    cluster.callApi().postSupervisor(spec);
    awaitRowsProcessed(10);

    // Realtime correctness (5 tenant_a, 5 IS NULL).
    assertNullTenantCounts();

    suspendAndAwaitHandoff(spec, 1);

    // Historical: the IS NULL query must still find the 5 null rows. If the DimensionValueSetShardSpec failed to declare
    // null, the broker would prune the segment here and return 0.
    assertNullTenantCounts();

    // The published shard spec declares the null value (serialized as a JSON null in the tenant list).
    verifyAllSegmentsHaveDimensionValueSetShardSpec(dataSource);
    final List<Map<String, Object>> shardSpecs = getShardSpecs(dataSource);
    final boolean someSpecDeclaresNull = shardSpecs.stream()
        .map(shardSpec -> {
          @SuppressWarnings("unchecked")
          final List<String> vals = ((Map<String, List<String>>) shardSpec.get("partitionDimensionValues")).get(COL_TENANT);
          return vals;
        })
        .anyMatch(vals -> vals != null && vals.contains(null));
    Assertions.assertTrue(
        someSpecDeclaresNull,
        "Expected at least one segment's partitionDimensionValues to declare a null tenant value: " + shardSpecs
    );
  }

  private ProducerRecord<byte[], byte[]> record(String topic, String csvTemplate, Object... args)
  {
    return new ProducerRecord<>(topic, 0, null, StringUtils.toUtf8(StringUtils.format(csvTemplate, args)));
  }

  private void assertRowCounts()
  {
    Assertions.assertEquals(
        String.valueOf(ROWS_PER_TENANT * 2), cluster.runSql("SELECT COUNT(*) FROM %s", dataSource));
    Assertions.assertEquals(
        String.valueOf(ROWS_PER_TENANT),
        cluster.runSql("SELECT COUNT(*) FROM %s WHERE %s = '%s'", dataSource, COL_TENANT, TENANT_A));
    Assertions.assertEquals(
        String.valueOf(ROWS_PER_TENANT),
        cluster.runSql("SELECT COUNT(*) FROM %s WHERE %s = '%s'", dataSource, COL_TENANT, TENANT_B));
  }

  /** Asserts 10 total rows split 5/5 between tenant_a and tenant_b (the mixed-tenant single-segment layout). */
  private void assertMixedTenantCounts()
  {
    Assertions.assertEquals("10", cluster.runSql("SELECT COUNT(*) FROM %s", dataSource));
    Assertions.assertEquals("5", cluster.runSql(
        "SELECT COUNT(*) FROM %s WHERE %s = '%s'", dataSource, COL_TENANT, TENANT_A));
    Assertions.assertEquals("5", cluster.runSql(
        "SELECT COUNT(*) FROM %s WHERE %s = '%s'", dataSource, COL_TENANT, TENANT_B));
  }

  private void assertNullTenantCounts()
  {
    Assertions.assertEquals("10", cluster.runSql("SELECT COUNT(*) FROM %s", dataSource));
    Assertions.assertEquals("5", cluster.runSql(
        "SELECT COUNT(*) FROM %s WHERE %s = '%s'", dataSource, COL_TENANT, TENANT_A));
    Assertions.assertEquals("5", cluster.runSql(
        "SELECT COUNT(*) FROM %s WHERE %s IS NULL", dataSource, COL_TENANT));
  }

  private void awaitRowsProcessed(long rows)
  {
    indexer.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("ingest/events/processed")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource),
        agg -> agg.hasSumAtLeast(rows)
    );
  }

  private void suspendAndAwaitHandoff(KafkaSupervisorSpec spec, long handoffs)
  {
    cluster.callApi().postSupervisor(spec.createSuspendedSpec());
    indexer.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("ingest/handoff/count")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource),
        agg -> agg.hasSumAtLeast(handoffs)
    );
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);
  }

  private KafkaSupervisorSpecBuilder newSupervisorBuilder()
  {
    return new KafkaSupervisorSpecBuilder()
        .withContext(Collections.singletonMap(Tasks.USE_CONCURRENT_LOCKS, true))
        .withDataSchema(
            schema -> schema
                .withTimestamp(new TimestampSpec(COL_TIMESTAMP, null, null))
                .withDimensions(
                    DimensionsSpec.builder()
                                  .setDimensions(List.of(
                                      new StringDimensionSchema(COL_TENANT),
                                      new StringDimensionSchema(COL_VALUE)
                                  ))
                                  .build()
                )
        )
        .withTuningConfig(
            tuningConfig -> tuningConfig
                .withMaxRowsPerSegment(1)
                .withReleaseLocksOnHandoff(true)
                .withStreamingPartitionsSpec(new StreamingPartitionsSpec(List.of(COL_TENANT)))
        )
        .withIoConfig(
            ioConfig -> ioConfig
                .withInputFormat(new CsvInputFormat(
                    List.of(COL_TIMESTAMP, COL_TENANT, COL_VALUE),
                    null, null, false, 0, false
                ))
                .withConsumerProperties(kafkaServer.consumerProperties())
                .withTaskDuration(Period.millis(500))
                .withStartDelay(Period.millis(10))
                .withSupervisorRunPeriod(Period.millis(500))
                .withCompletionTimeout(Period.seconds(5))
                .withUseEarliestSequenceNumber(true)
        );
  }

  private KafkaSupervisorSpec dayGranularitySupervisor(String topic, List<String> partitionFilterDims)
  {
    return new KafkaSupervisorSpecBuilder()
        .withContext(Collections.singletonMap(Tasks.USE_CONCURRENT_LOCKS, true))
        .withDataSchema(
            schema -> schema
                .withTimestamp(new TimestampSpec(COL_TIMESTAMP, null, null))
                .withDimensions(
                    DimensionsSpec.builder()
                                  .setDimensions(List.of(
                                      new StringDimensionSchema(COL_TENANT),
                                      new StringDimensionSchema(COL_VALUE)
                                  ))
                                  .build()
                )
                .withGranularity(new UniformGranularitySpec(Granularities.DAY, Granularities.NONE, null))
        )
        .withTuningConfig(t -> {
          t.withMaxRowsPerSegment(1000).withReleaseLocksOnHandoff(true);
          if (!partitionFilterDims.isEmpty()) {
            t.withStreamingPartitionsSpec(new StreamingPartitionsSpec(partitionFilterDims));
          }
        })
        .withIoConfig(
            ioConfig -> ioConfig
                .withInputFormat(new CsvInputFormat(
                    List.of(COL_TIMESTAMP, COL_TENANT, COL_VALUE), null, null, false, 0, false))
                .withConsumerProperties(kafkaServer.consumerProperties())
                .withTaskDuration(Period.millis(500))
                .withStartDelay(Period.millis(10))
                .withSupervisorRunPeriod(Period.millis(500))
                .withCompletionTimeout(Period.seconds(5))
                .withUseEarliestSequenceNumber(true)
        )
        .withId(dataSource + "_supe")
        .build(dataSource, topic);
  }

  private List<ProducerRecord<byte[], byte[]>> generateRecords(
      String topic,
      String tenantValue,
      int numRecords,
      DateTime startTime
  )
  {
    final List<ProducerRecord<byte[], byte[]>> records = new ArrayList<>();
    for (int i = 0; i < numRecords; i++) {
      final String csv = StringUtils.format(
          "%s,%s,value_%d",
          startTime.plusHours(i),
          tenantValue,
          i
      );
      records.add(new ProducerRecord<>(topic, 0, null, StringUtils.toUtf8(csv)));
    }
    return records;
  }

  private void verifyAllSegmentsHaveDimensionValueSetShardSpec(String ds)
  {
    verifyAllSegmentsHaveShardSpecType(ds, "dim_value_set");
  }

  private void verifyAllSegmentsHaveShardSpecType(String ds, String expectedType)
  {
    final List<Map<String, Object>> specs = getShardSpecs(ds);
    for (Map<String, Object> spec : specs) {
      Assertions.assertEquals(
          expectedType,
          spec.get("type"),
          "Expected " + expectedType + " shard spec but got: " + spec
      );
    }
  }

  private List<Map<String, Object>> getShardSpecs(String ds)
  {
    final String csv = cluster.runSql(
        "SELECT shard_spec FROM sys.segments"
        + " WHERE datasource = '%s' AND is_overshadowed = 0 AND is_available = 1",
        ds
    );
    final ObjectMapper mapper = new ObjectMapper();
    final List<Map<String, Object>> specs = new ArrayList<>();
    for (String line : csv.split("\n")) {
      String trimmed = line.trim();
      if (trimmed.isEmpty()) {
        continue;
      }
      // runSql returns CSV-quoted JSON — strip surrounding quotes and unescape doubled quotes
      if (trimmed.startsWith("\"") && trimmed.endsWith("\"")) {
        trimmed = StringUtils.replace(trimmed.substring(1, trimmed.length() - 1), "\"\"", "\"");
      }
      try {
        specs.add(mapper.readValue(trimmed, new TypeReference<>() {}));
      }
      catch (Exception e) {
        Assertions.fail("Failed to parse shard_spec JSON: " + trimmed + " — " + e.getMessage());
      }
    }
    Assertions.assertFalse(specs.isEmpty(), "Expected at least one segment in sys.segments for " + ds);
    return specs;
  }
}
