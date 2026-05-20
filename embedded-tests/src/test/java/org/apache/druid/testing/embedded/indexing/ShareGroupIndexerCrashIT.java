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
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.granularity.UniformGranularitySpec;
import org.apache.druid.indexing.kafka.KafkaIndexTaskModule;
import org.apache.druid.indexing.kafka.KafkaIndexTaskTuningConfig;
import org.apache.druid.indexing.kafka.ShareGroupIndexTask;
import org.apache.druid.indexing.kafka.ShareGroupIndexTaskIOConfig;
import org.apache.druid.jackson.DefaultObjectMapper;
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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Verifies at-least-once semantics: uncommitted records are redelivered after Indexer crash.
 */
public class ShareGroupIndexerCrashIT extends EmbeddedClusterTestBase
{
  private static final long SHARE_CONSUMER_READY_DELAY_MS = 3_000L;
  private static final String COL_TIMESTAMP = "__time";
  private static final String COL_ITEM = "item";
  private static final String COL_VALUE = "value";
  private static final String GROUP_ID = "crash-it-group";

  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer();
  private final EmbeddedHistorical historical = new EmbeddedHistorical();
  private final EmbeddedBroker broker = new EmbeddedBroker();

  private ShareGroupKafkaResource kafkaServer;
  private final ObjectMapper mapper = new DefaultObjectMapper();

  @Override
  public EmbeddedDruidCluster createCluster()
  {
    kafkaServer = new ShareGroupKafkaResource();
    final EmbeddedDruidCluster cluster = EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper();
    indexer.addProperty("druid.segment.handoff.pollDuration", "PT0.1s");
    cluster.addExtension(KafkaIndexTaskModule.class)
           .addResource(kafkaServer)
           .useLatchableEmitter()
           .useDefaultTimeoutForLatchableEmitter(60)
           .addCommonProperty("druid.monitoring.emissionPeriod", "PT0.1s")
           .addServer(coordinator)
           .addServer(overlord)
           .addServer(indexer)
           .addServer(historical)
           .addServer(broker);
    return cluster;
  }

  @Test
  public void test_indexerCrash_midIngestion_noDataLossAfterRecovery() throws Exception
  {
    final String topic = dataSource + "_crash_topic";
    kafkaServer.createTopicWithPartitions(topic, 1);
    kafkaServer.setShareGroupAutoOffsetReset(GROUP_ID, "earliest");

    // Produce all records before starting task so Indexer is polling them when it crashes.
    final int batchA = 20;
    kafkaServer.publishRecordsToTopic(topic, csvRecords(batchA, 0, "2025-09-01"));

    // Task 1: starts ingesting but Indexer crashes before it completes.
    final String taskId1 = submitTask(topic);
    Thread.sleep(SHARE_CONSUMER_READY_DELAY_MS);

    // Wait for at least some rows to be processed (but not all published to segments).
    indexer.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("ingest/events/processed")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource),
        agg -> agg.hasSumAtLeast(1)
    );

    // Simulate JVM crash: stop the indexer abruptly (no graceful shutdown).
    indexer.stop();

    // Restart the indexer.
    indexer.start();

    // Task 2: same group-id. Broker redelivers any unacknowledged records.
    final String taskId2 = submitTask(topic);
    Thread.sleep(SHARE_CONSUMER_READY_DELAY_MS);

    // Produce a second batch to ensure Task 2 is actively consuming.
    final int batchB = 10;
    kafkaServer.publishRecordsToTopic(topic, csvRecords(batchB, batchA, "2025-09-02"));

    // Give Task 2 time to consume redelivered + new records.
    // Post-restart emitter has a fresh counter, so we use a dwell + SQL check instead.
    Thread.sleep(15_000L);

    cluster.callApi().onLeaderOverlord(o -> o.cancelTask(taskId2));
    cluster.callApi().waitForTaskToFinish(taskId2, overlord.latchableEmitter());

    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);

    // At-least-once guarantee: row count >= batchA + batchB (no loss); may be slightly higher due to redelivery.
    final long rowCount = Long.parseLong(cluster.runSql("SELECT COUNT(*) FROM %s", dataSource));
    Assertions.assertTrue(
        rowCount >= batchA + batchB,
        "Expected at least [" + (batchA + batchB) + "] rows but got [" + rowCount + "]"
    );
  }

  @Test
  public void test_indexerCrash_beforeAnyAck_allRecordsRedelivered() throws Exception
  {
    final String topic = dataSource + "_crash_no_ack_topic";
    kafkaServer.createTopicWithPartitions(topic, 1);
    kafkaServer.setShareGroupAutoOffsetReset(GROUP_ID + "-noack", "earliest");

    final int numRecords = 10;
    kafkaServer.publishRecordsToTopic(topic, csvRecords(numRecords, 0, "2025-10-01"));

    // Task 1: starts but is killed before its first publish (no ack committed).
    submitTask(topic, GROUP_ID + "-noack");
    Thread.sleep(SHARE_CONSUMER_READY_DELAY_MS);

    indexer.stop();
    indexer.start();

    // Task 2 (same group): broker must redeliver all numRecords because none were acked.
    final String taskId2 = submitTask(topic, GROUP_ID + "-noack");
    Thread.sleep(SHARE_CONSUMER_READY_DELAY_MS);

    // Give Task 2 time to consume redelivered records before cancel.
    Thread.sleep(15_000L);

    cluster.callApi().onLeaderOverlord(o -> o.cancelTask(taskId2));
    cluster.callApi().waitForTaskToFinish(taskId2, overlord.latchableEmitter());

    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);

    final long rowCount = Long.parseLong(cluster.runSql("SELECT COUNT(*) FROM %s", dataSource));
    Assertions.assertTrue(
        rowCount >= numRecords,
        "Expected at least [" + numRecords + "] rows after crash recovery but got [" + rowCount + "]"
    );
  }

  private String submitTask(String topic)
  {
    return submitTask(topic, GROUP_ID);
  }

  private String submitTask(String topic, String groupId)
  {
    final Map<String, Object> consumerProps = kafkaServer.consumerProperties();
    final ShareGroupIndexTaskIOConfig ioConfig = new ShareGroupIndexTaskIOConfig(
        topic,
        groupId,
        consumerProps,
        new CsvInputFormat(
            List.of(COL_TIMESTAMP, COL_ITEM, COL_VALUE),
            null,
            null,
            false,
            0,
            false
        ),
        null
    );
    final DataSchema dataSchema = DataSchema.builder()
                                            .withDataSource(dataSource)
                                            .withTimestamp(new TimestampSpec(COL_TIMESTAMP, "auto", null))
                                            .withDimensions(
                                                DimensionsSpec.builder()
                                                              .setDimensions(
                                                                  DimensionsSpec.getDefaultSchemas(
                                                                      List.of(COL_ITEM, COL_VALUE)
                                                                  )
                                                              )
                                                              .build()
                                            )
                                            .withGranularity(
                                                new UniformGranularitySpec(
                                                    Granularities.DAY,
                                                    Granularities.NONE,
                                                    null
                                                )
                                            )
                                            .build();
    final ShareGroupIndexTask task = new ShareGroupIndexTask(
        null,
        null,
        dataSchema,
        new KafkaIndexTaskTuningConfig(
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
        ),
        ioConfig,
        null,
        mapper
    );
    cluster.callApi().submitTask(task);
    return task.getId();
  }

  private List<byte[]> csvRecords(int count, int startIndex, String dateStr)
  {
    final List<byte[]> records = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      final String csv = dateStr + "T00:" + StringUtils.format("%02d", (startIndex + i) % 60) + ":00Z"
                         + ",item" + (startIndex + i) + "," + (startIndex + i);
      records.add(csv.getBytes(StandardCharsets.UTF_8));
    }
    return records;
  }
}
