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

package org.apache.druid.testing.embedded.indexing;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/** End-to-end IT for Kafka share-group ingestion; requires Docker. */
public class EmbeddedShareGroupIngestionTest extends EmbeddedClusterTestBase
{
  private static final String COL_TIMESTAMP = "__time";
  private static final String COL_ITEM = "item";
  private static final String COL_VALUE = "value";

  private static final InputFormat DEFAULT_CSV_FORMAT =
      new CsvInputFormat(List.of(COL_TIMESTAMP, COL_ITEM, COL_VALUE), null, null, false, 0, false);

  /** Lets a freshly submitted share-group task reach STABLE before records are produced. */
  private static final long SHARE_CONSUMER_READY_DELAY_MS = 3_000L;

  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer();
  private final EmbeddedHistorical historical = new EmbeddedHistorical();
  private final EmbeddedBroker broker = new EmbeddedBroker();

  private ShareGroupKafkaResource kafkaServer;

  @Override
  public EmbeddedDruidCluster createCluster()
  {
    kafkaServer = new ShareGroupKafkaResource();

    final EmbeddedDruidCluster cluster = EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper();
    indexer.addProperty("druid.segment.handoff.pollDuration", "PT0.1s");

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

    Thread.sleep(SHARE_CONSUMER_READY_DELAY_MS);

    final int numRecords = 10;
    kafkaServer.produceRecordsToTopic(
        generateRecords(topic, numRecords, DateTimes.of("2025-06-01"))
    );

    waitForRowsProcessed(numRecords);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);
    assertRowCountEventually(numRecords);

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

    Thread.sleep(SHARE_CONSUMER_READY_DELAY_MS);

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

    Thread.sleep(SHARE_CONSUMER_READY_DELAY_MS);

    final int numRecords = 10;
    kafkaServer.produceRecordsToTopic(
        generateRecords(topic, numRecords, DateTimes.of("2025-08-01"))
    );

    waitForRowsProcessed(numRecords);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);
    assertRowCountEventually(numRecords);

    cancelAndAwaitTermination(taskId);
  }

  /** Cancel must drain the in-flight batch so rows ingested before cancel remain queryable. */
  @Test
  public void test_shareGroupIngestion_gracefulStop_publishesInflightBatch() throws InterruptedException
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

    Thread.sleep(SHARE_CONSUMER_READY_DELAY_MS);

    final int numRecords = 5;
    kafkaServer.produceRecordsToTopic(
        generateRecords(topic, numRecords, DateTimes.of("2025-09-01"))
    );

    waitForRowsProcessed(numRecords);

    cancelAndAwaitTermination(taskId);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);
    assertRowCountEventually(numRecords);
  }

  /** Three tasks sharing one group on a 2-partition topic ingest every record exactly once (KIP-932). */
  @Test
  public void test_shareGroupIngestion_multiTaskShareGroup_noDuplicatesNoLoss() throws InterruptedException
  {
    final String topic = dataSource + "_topic";
    kafkaServer.createTopicWithPartitions(topic, 2);

    final String groupId = "druid-share-group-fanout";
    final String taskA = submitShareGroupTask(
        topic,
        groupId,
        kafkaServer.consumerProperties(),
        DEFAULT_CSV_FORMAT,
        defaultTuningConfig()
    );
    final String taskB = submitShareGroupTask(
        topic,
        groupId,
        kafkaServer.consumerProperties(),
        DEFAULT_CSV_FORMAT,
        defaultTuningConfig()
    );
    final String taskC = submitShareGroupTask(
        topic,
        groupId,
        kafkaServer.consumerProperties(),
        DEFAULT_CSV_FORMAT,
        defaultTuningConfig()
    );

    Thread.sleep(SHARE_CONSUMER_READY_DELAY_MS);

    final int numRecords = 30;
    kafkaServer.produceRecordsToTopic(
        generateRecords(topic, numRecords, DateTimes.of("2025-10-01"))
    );

    waitForRowsProcessed(numRecords);

    cancelAndAwaitTermination(taskA);
    cancelAndAwaitTermination(taskB);
    cancelAndAwaitTermination(taskC);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);
    assertRowCountEventually(numRecords);
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

    Thread.sleep(SHARE_CONSUMER_READY_DELAY_MS);

    final int numRecords = 1000;
    kafkaServer.produceRecordsToTopic(
        generateRecords(topic, numRecords, DateTimes.of("2025-11-01"))
    );

    waitForRowsProcessed(numRecords);

    cancelAndAwaitTermination(taskId);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);
    assertRowCountEventually(numRecords);
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
  }

  private void waitForRowsProcessed(long expected)
  {
    indexer.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("ingest/events/processed")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource),
        agg -> agg.hasSumAtLeast(expected)
    );
  }

  /**
   * Polls SQL for the expected row count, swallowing transient
   * "datasource not found" errors that can arise immediately after a task
   * cancel before the broker's SQL catalog has refreshed.
   */
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
          startTime.plusDays(i),
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
