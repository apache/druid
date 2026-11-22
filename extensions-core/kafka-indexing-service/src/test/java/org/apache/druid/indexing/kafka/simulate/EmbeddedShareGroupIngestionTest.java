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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.client.DruidServer;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.granularity.UniformGranularitySpec;
import org.apache.druid.indexing.kafka.KafkaIndexTaskModule;
import org.apache.druid.indexing.kafka.KafkaIndexTaskTuningConfig;
import org.apache.druid.indexing.kafka.ShareGroupIndexTask;
import org.apache.druid.indexing.kafka.ShareGroupIndexTaskIOConfig;
import org.apache.druid.indexing.kafka.supervisor.KafkaTuningConfigBuilder;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.seekablestream.DimensionValueSetPartitionsSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.server.coordinator.rules.ForeverLoadRule;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.DimensionValueSetShardSpec;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** End-to-end IT for Kafka share-group ingestion; requires Docker. */
public class EmbeddedShareGroupIngestionTest extends EmbeddedClusterTestBase
{
  private static final String COL_TIMESTAMP = "__time";
  private static final String COL_ITEM = "item";
  private static final String COL_VALUE = "value";

  private static final InputFormat DEFAULT_CSV_FORMAT =
      new CsvInputFormat(List.of(COL_TIMESTAMP, COL_ITEM, COL_VALUE), null, null, false, 0, false);

  /** Lets a freshly submitted share-group task reach STABLE before records are produced. */

  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer();
  private final EmbeddedHistorical historical = new EmbeddedHistorical();
  private final EmbeddedBroker broker = new EmbeddedBroker();

  private ShareGroupKafkaResource kafkaServer;

  @Override
  public EmbeddedDruidCluster createCluster()
  {
    ShareGroupKafkaResource.assignAvailablePorts(coordinator, overlord, indexer, historical, broker);
    kafkaServer = new ShareGroupKafkaResource();

    final EmbeddedDruidCluster cluster = EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper();
    indexer.addProperty("druid.segment.handoff.pollDuration", "PT0.1s");
    indexer.addProperty("druid.indexer.task.gracefulShutdownTimeout", "PT5S");

    cluster.addExtension(KafkaIndexTaskModule.class)
           .addResource(kafkaServer)
           .useLatchableEmitter()
           .useDefaultTimeoutForLatchableEmitter(30)
           .addCommonProperty("druid.monitoring.emissionPeriod", "PT0.1s")
           .addServer(coordinator)
           .addServer(overlord)
           .addServer(indexer)
           .addServer(historical)
           .addServer(broker);

    return cluster;
  }

  @Test
  public void test_shareGroupIngestion_basicEndToEnd() throws InterruptedException
  {
    final String topic = dataSource + "_topic";
    kafkaServer.createTopicWithPartitions(topic, 2);

    final String taskId = submitShareGroupTask(
        topic,
        "druid-share-group-test",
        kafkaServer.consumerProperties(),
        DEFAULT_CSV_FORMAT,
        defaultTuningConfig()
    );

    final int numRecords = 10;
    kafkaServer.produceRecordsToTopic(
        generateRecords(topic, numRecords, DateTimes.of("2025-06-01"))
    );

    waitForRowsProcessed(numRecords);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);
    assertRowCountEventually(numRecords);

    cancelAndAwaitTermination(taskId);
  }

  @Test
  public void test_shareGroupIngestion_prunesPublishedSegments()
  {
    final String topic = dataSource + "_topic";
    kafkaServer.createTopicWithPartitions(topic, 1);
    final Map<String, Object> consumerProperties = new HashMap<>(kafkaServer.consumerProperties());
    consumerProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
    final KafkaIndexTaskTuningConfig tuningConfig = new KafkaTuningConfigBuilder()
        .withStreamingPartitionsSpec(new DimensionValueSetPartitionsSpec(List.of(COL_ITEM)))
        .build()
        .convertToTaskTuningConfig();
    final String taskId = submitShareGroupTask(
        topic,
        dataSource + "_group",
        consumerProperties,
        DEFAULT_CSV_FORMAT,
        tuningConfig
    );

    kafkaServer.publishRecordsToTopic(
        topic,
        List.of(
            StringUtils.toUtf8("2025-06-01T00:00:01Z,acme,1"),
            StringUtils.toUtf8("2025-06-01T00:00:02Z,gamma,2")
        )
    );
    indexer.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("ingest/kafka/shareGroup/acknowledged").hasDimension(DruidMetrics.TASK_ID, taskId),
        agg -> agg.hasSumAtLeast(2)
    );
    indexer.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("ingest/handoff/count").hasDimension(DruidMetrics.DATASOURCE, dataSource),
        agg -> agg.hasSumAtLeast(2)
    );
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);

    final Set<DataSegment> segments = cluster.callApi().getVisibleUsedSegments(dataSource, overlord);
    Assertions.assertEquals(2, segments.size());
    final Map<List<String>, String> segmentIdByItem = new HashMap<>();
    for (final DataSegment segment : segments) {
      final DimensionValueSetShardSpec shardSpec = Assertions.assertInstanceOf(
          DimensionValueSetShardSpec.class,
          segment.getShardSpec()
      );
      segmentIdByItem.put(shardSpec.getPartitionDimensionValues().get(COL_ITEM), segment.getId().toString());
    }
    Assertions.assertEquals(Set.of(List.of("acme"), List.of("gamma")), segmentIdByItem.keySet());

    Assertions.assertEquals("1", cluster.runSql("SELECT COUNT(*) FROM %s WHERE %s = 'acme'", dataSource, COL_ITEM));
    final String queryId = (String) historical.latchableEmitter().waitForEvent(
        event -> event.hasMetricName("query/time")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource)
                      .hasDimension(DruidMetrics.TYPE, "timeseries")
    ).getUserDims().get(DruidMetrics.ID);
    Assertions.assertNotNull(queryId);
    final Set<String> scannedSegments = historical.latchableEmitter().getMetricEvents("query/segment/time").stream()
        .filter(event -> queryId.equals(event.getUserDims().get(DruidMetrics.ID)))
        .map(event -> (String) event.getUserDims().get("segment"))
        .collect(Collectors.toSet());
    Assertions.assertEquals(Set.of(segmentIdByItem.get(List.of("acme"))), scannedSegments);
    cancelAndAwaitTermination(taskId);
  }

  /** Verifies that one Kafka record carrying multiple JSON objects yields one row per object. */
  @Test
  public void test_shareGroupIngestion_multiRowPerKafkaRecord() throws InterruptedException
  {
    final String topic = dataSource + "_topic";
    kafkaServer.createTopicWithPartitions(topic, 2);

    final String taskId = submitShareGroupTask(
        topic,
        "druid-share-group-multirow-test",
        kafkaServer.consumerProperties(),
        new JsonInputFormat(null, Collections.emptyMap(), null, null, true),
        defaultTuningConfig()
    );

    final int numRecords = 5;
    final int rowsPerRecord = 4;
    kafkaServer.produceRecordsToTopic(
        generateMultiObjectJsonRecords(topic, numRecords, rowsPerRecord, DateTimes.of("2025-07-01"))
    );

    final long expectedRows = (long) numRecords * rowsPerRecord;
    waitForRowsProcessed(expectedRows);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);
    assertRowCountEventually(expectedRows);

    cancelAndAwaitTermination(taskId);
  }

  /** Forbidden share-group consumer properties are stripped so the task starts and ingests. */
  @Test
  public void test_shareGroupIngestion_unsupportedConsumerProperty_isSanitized() throws InterruptedException
  {
    final String topic = dataSource + "_topic";
    kafkaServer.createTopicWithPartitions(topic, 2);

    final String taskId = submitShareGroupTask(
        topic,
        "druid-share-group-sanitize-test",
        consumerPropsWithUnsupportedKeys(),
        DEFAULT_CSV_FORMAT,
        defaultTuningConfig()
    );

    final int numRecords = 10;
    kafkaServer.produceRecordsToTopic(
        generateRecords(topic, numRecords, DateTimes.of("2025-08-01"))
    );

    waitForRowsProcessed(numRecords);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);
    assertRowCountEventually(numRecords);

    cancelAndAwaitTermination(taskId);
  }

  /** Graceful stop preserves a batch that has already become queryable. */
  @Test
  public void test_shareGroupIngestion_gracefulStop_preservesPublishedBatch() throws InterruptedException
  {
    final String topic = dataSource + "_topic";
    kafkaServer.createTopicWithPartitions(topic, 2);

    final String taskId = submitShareGroupTask(
        topic,
        "druid-share-group-graceful-stop",
        kafkaServer.consumerProperties(),
        DEFAULT_CSV_FORMAT,
        defaultTuningConfig()
    );

    final int numRecords = 5;
    kafkaServer.produceRecordsToTopic(
        generateRecords(topic, numRecords, DateTimes.of("2025-09-01"))
    );

    waitForRowsProcessed(numRecords);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);
    assertRowCountEventually(numRecords);
    cancelAndAwaitTermination(taskId);
  }

  @Test
  public void test_shareGroupIngestion_resumesAfterHandoffStall() throws InterruptedException
  {
    final String topic = dataSource + "_topic";
    final String groupId = dataSource + "_group";
    kafkaServer.createTopicWithPartitions(topic, 1);
    cluster.callApi().onLeaderCoordinator(c -> c.updateRulesForDatasource(
        dataSource,
        List.of(new ForeverLoadRule(Map.of("unavailable-tier", 1), null))
    ));

    final Map<String, Object> consumerProperties = new HashMap<>(kafkaServer.consumerProperties());
    consumerProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
    consumerProperties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 5_000);
    final String taskId = submitShareGroupTask(topic, groupId, consumerProperties, DEFAULT_CSV_FORMAT, defaultTuningConfig());

    kafkaServer.publishRecordsToTopic(
        topic,
        generateRecords(topic, 9, DateTimes.of("2025-09-02")).stream().map(ProducerRecord::value).toList()
    );
    final String ackMetric = "ingest/kafka/shareGroup/acknowledged";
    indexer.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName(ackMetric).hasDimension(DruidMetrics.TASK_ID, taskId),
        aggregate -> aggregate.hasSumAtLeast(8)
    );
    try (final Admin admin = kafkaServer.newAdminClient()) {
      cluster.callApi().waitForResult(
          () -> admin.describeShareGroups(List.of(groupId)).describedGroups().get(groupId)
                     .get(5, TimeUnit.SECONDS).members().size(),
          count -> count == 0
      ).withTimeoutMillis(30_000).go();
    }
    Assertions.assertEquals(
        8L,
        indexer.latchableEmitter().getMetricValues(ackMetric, Map.of(DruidMetrics.TASK_ID, taskId))
               .stream().mapToLong(Number::longValue).sum()
    );

    cluster.callApi().onLeaderCoordinator(c -> c.updateRulesForDatasource(
        dataSource,
        List.of(new ForeverLoadRule(Map.of(DruidServer.DEFAULT_TIER, 1), null))
    ));
    indexer.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName(ackMetric).hasDimension(DruidMetrics.TASK_ID, taskId),
        aggregate -> aggregate.hasSumAtLeast(9)
    );
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);
    assertRowCountEventually(9);
    cancelAndAwaitTermination(taskId);
  }

  @Test
  public void test_shareGroupIngestion_moreTasksThanPartitions() throws InterruptedException
  {
    final String topic = dataSource + "_topic";
    kafkaServer.createTopicWithPartitions(topic, 1);

    final String groupId = "druid-share-group-fanout";
    final Map<String, Object> consumerProperties = new HashMap<>(kafkaServer.consumerProperties());
    consumerProperties.put("max.poll.records", 4);
    final String taskA = submitShareGroupTask(
        topic,
        groupId,
        consumerProperties,
        DEFAULT_CSV_FORMAT,
        defaultTuningConfig()
    );
    final String taskB = submitShareGroupTask(
        topic,
        groupId,
        consumerProperties,
        DEFAULT_CSV_FORMAT,
        defaultTuningConfig()
    );
    final String taskC = submitShareGroupTask(
        topic,
        groupId,
        consumerProperties,
        DEFAULT_CSV_FORMAT,
        defaultTuningConfig()
    );

    try (final Admin admin = kafkaServer.newAdminClient()) {
      cluster.callApi().waitForResult(
          () -> admin.describeShareGroups(List.of(groupId)).describedGroups().get(groupId)
                     .get(5, TimeUnit.SECONDS).members().size(),
          count -> count == 3
      ).withTimeoutMillis(30_000).go();
    }

    final int numRecords = 30;
    kafkaServer.publishRecordsToTopic(
        topic,
        generateRecords(topic, numRecords, DateTimes.of("2025-10-01")).stream().map(ProducerRecord::value).toList()
    );
    final String ackMetric = "ingest/kafka/shareGroup/acknowledged";
    indexer.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName(ackMetric).hasDimension(DruidMetrics.DATASOURCE, dataSource),
        aggregate -> aggregate.hasSumAtLeast(numRecords)
    );
    final long tasksWithAcknowledgements = List.of(taskA, taskB, taskC).stream()
        .filter(taskId -> !indexer.latchableEmitter().getMetricValues(ackMetric, Map.of(DruidMetrics.TASK_ID, taskId)).isEmpty())
        .count();
    Assertions.assertTrue(tasksWithAcknowledgements > 1);

    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);
    assertRowCountEventually(numRecords);

    cancelAndAwaitTermination(taskA);
    cancelAndAwaitTermination(taskB);
    cancelAndAwaitTermination(taskC);
  }

  /** 1000 rows with {@code maxRowsInMemory=100} forces mid-batch persists yet ingests all rows. */
  @Test
  public void test_shareGroupIngestion_lowMaxRowsInMemory_dataCorrect() throws InterruptedException
  {
    final String topic = dataSource + "_topic";
    kafkaServer.createTopicWithPartitions(topic, 2);

    final String taskId = submitShareGroupTask(
        topic,
        "druid-share-group-persist-test",
        kafkaServer.consumerProperties(),
        DEFAULT_CSV_FORMAT,
        tuningConfigWithMaxRowsInMemory(100)
    );

    final int numRecords = 1000;
    kafkaServer.produceRecordsToTopic(
        generateRecords(topic, numRecords, DateTimes.of("2025-11-01"))
    );

    waitForRowsProcessed(numRecords);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);
    assertRowCountEventually(numRecords);
    cancelAndAwaitTermination(taskId);
  }

  private DataSchema buildDataSchema()
  {
    return DataSchema.builder()
        .withDataSource(dataSource)
        .withTimestamp(new TimestampSpec(COL_TIMESTAMP, null, null))
        .withDimensions(DimensionsSpec.EMPTY)
        .withGranularity(new UniformGranularitySpec(
            Granularities.DAY,
            Granularities.NONE,
            false,
            null
        ))
        .build();
  }

  private static KafkaIndexTaskTuningConfig defaultTuningConfig()
  {
    return new KafkaIndexTaskTuningConfig(
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

  private static KafkaIndexTaskTuningConfig tuningConfigWithMaxRowsInMemory(int maxRowsInMemory)
  {
    return new KafkaIndexTaskTuningConfig(
        null,
        maxRowsInMemory,
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

  private String submitShareGroupTask(
      String topic,
      String groupId,
      Map<String, Object> consumerProperties,
      InputFormat inputFormat,
      KafkaIndexTaskTuningConfig tuningConfig
  )
  {
    // Broker default is LATEST; deliver pre-existing records to this group.
    kafkaServer.setShareGroupAutoOffsetReset(groupId, "earliest");

    final ShareGroupIndexTaskIOConfig ioConfig = new ShareGroupIndexTaskIOConfig(
        topic,
        groupId,
        consumerProperties,
        inputFormat,
        null
    );

    final ObjectMapper mapper = new DefaultObjectMapper();
    final ShareGroupIndexTask task = new ShareGroupIndexTask(
        null,
        null,
        buildDataSchema(),
        tuningConfig,
        ioConfig,
        null,
        mapper
    );

    cluster.callApi().submitTask(task);
    return task.getId();
  }

  /**
   * Cancels and waits for any terminal state. User-initiated cancels are
   * always reported as FAILED by the Overlord; only supervisors can drive
   * SUCCESS via {@code shutdownWithSuccess}, so data correctness is verified
   * separately via SQL row counts.
   */
  private void cancelAndAwaitTermination(String taskId)
  {
    cluster.callApi().onLeaderOverlord(o -> o.cancelTask(taskId));
    cluster.callApi().waitForTaskToFinish(taskId, overlord.latchableEmitter());
    waitForTaskRunnerRemoval(taskId);
  }

  private void waitForTaskRunnerRemoval(String taskId)
  {
    final TaskRunner taskRunner = indexer.bindings().getInstance(TaskRunner.class);
    final long deadlineMillis = System.currentTimeMillis() + 10_000L;
    while (System.currentTimeMillis() < deadlineMillis) {
      final boolean taskPresent = taskRunner.getKnownTasks()
                                            .stream()
                                            .anyMatch(task -> taskId.equals(task.getTaskId()));
      if (!taskPresent) {
        return;
      }

      try {
        Thread.sleep(100L);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        Assertions.fail("Interrupted while waiting for task runner cleanup", e);
      }
    }

    Assertions.fail("Task [" + taskId + "] remained in the indexer task runner after cancellation");
  }

  private void waitForRowsProcessed(long expected)
  {
    indexer.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("ingest/events/processed")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource),
        agg -> agg.hasSumAtLeast(expected)
    );
  }

  /** Polls SQL for the expected row count while the broker schema becomes available. */
  private void assertRowCountEventually(long expected)
  {
    final String expectedStr = String.valueOf(expected);
    final long deadlineMillis = System.currentTimeMillis() + 30_000L;
    String last = null;
    Exception lastException = null;
    while (System.currentTimeMillis() < deadlineMillis) {
      try {
        last = cluster.runSql("SELECT COUNT(*) FROM %s", dataSource);
        if (expectedStr.equals(last)) {
          return;
        }
        lastException = null;
      }
      catch (Exception e) {
        lastException = e;
      }
      try {
        Thread.sleep(250L);
      }
      catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    Assertions.fail(
        "Expected row count [" + expectedStr + "] but last result was [" + last
        + "], lastException=" + lastException
    );
  }

  /** Builds a property map containing keys forbidden by {@code SHARE_GROUP_UNSUPPORTED_CONFIGS}. */
  private Map<String, Object> consumerPropsWithUnsupportedKeys()
  {
    final Map<String, Object> props = new HashMap<>(kafkaServer.consumerProperties());
    props.put("enable.auto.commit", "false");
    props.put("auto.offset.reset", "earliest");
    props.put("group.instance.id", "share-it-instance-0");
    return props;
  }

  private List<ProducerRecord<byte[], byte[]>> generateRecords(
      String topic,
      int numRecords,
      DateTime startTime
  )
  {
    final List<ProducerRecord<byte[], byte[]>> records = new ArrayList<>();
    for (int i = 0; i < numRecords; i++) {
      final String csv = StringUtils.format(
          "%s,item_%d,%d",
          startTime.plusSeconds(i),
          i,
          ThreadLocalRandom.current().nextInt(1000)
      );
      records.add(
          new ProducerRecord<>(topic, i % 2, null, StringUtils.toUtf8(csv))
      );
    }
    return records;
  }

  private List<ProducerRecord<byte[], byte[]>> generateMultiObjectJsonRecords(
      String topic,
      int numRecords,
      int rowsPerRecord,
      DateTime startTime
  )
  {
    final List<ProducerRecord<byte[], byte[]>> records = new ArrayList<>();
    for (int i = 0; i < numRecords; i++) {
      final StringBuilder json = new StringBuilder();
      for (int j = 0; j < rowsPerRecord; j++) {
        if (j > 0) {
          json.append(' ');
        }
        json.append(StringUtils.format(
            "{\"%s\":\"%s\",\"%s\":\"item_%d_%d\",\"%s\":%d}",
            COL_TIMESTAMP,
            startTime.plusHours(i * rowsPerRecord + j),
            COL_ITEM,
            i,
            j,
            COL_VALUE,
            ThreadLocalRandom.current().nextInt(1000)
        ));
      }
      records.add(new ProducerRecord<>(topic, i % 2, null, StringUtils.toUtf8(json.toString())));
    }
    return records;
  }
}
