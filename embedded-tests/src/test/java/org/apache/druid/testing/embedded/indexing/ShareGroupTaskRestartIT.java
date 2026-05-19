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

/** Verifies that share-group broker state survives task death so a restarted task does not re-ingest already-committed records. */
public class ShareGroupTaskRestartIT extends EmbeddedClusterTestBase
{
  private static final long SHARE_CONSUMER_READY_DELAY_MS = 3_000L;
  private static final String COL_TIMESTAMP = "__time";
  private static final String COL_ITEM = "item";
  private static final String COL_VALUE = "value";
  private static final String GROUP_ID = "restart-it-group";

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
  public void test_restartedTask_sameGroupId_doesNotReingestAlreadyCommittedRecords() throws Exception
  {
    final String topic = dataSource + "_restart_topic";
    kafkaServer.createTopicWithPartitions(topic, 1);

    // Task 1: ingest 10 records, then stop gracefully so offsets are committed.
    kafkaServer.setShareGroupAutoOffsetReset(GROUP_ID, "earliest");
    final String taskId1 = submitTask(topic);
    Thread.sleep(SHARE_CONSUMER_READY_DELAY_MS);

    final int batchA = 10;
    kafkaServer.publishRecordsToTopic(topic, csvRecords(batchA, 0, "2025-07-01"));

    waitForRowsProcessed(batchA);

    cluster.callApi().onLeaderOverlord(o -> o.cancelTask(taskId1));
    cluster.callApi().waitForTaskToFinish(taskId1, overlord.latchableEmitter());

    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);
    assertRowCountEventually(batchA);

    // Task 2: same group-id, no offset-reset override — broker delivers only NEW records.
    final String taskId2 = submitTask(topic);
    Thread.sleep(SHARE_CONSUMER_READY_DELAY_MS);

    final int batchB = 10;
    kafkaServer.publishRecordsToTopic(topic, csvRecords(batchB, batchA, "2025-07-02"));

    waitForRowsProcessed(batchA + batchB);

    cluster.callApi().onLeaderOverlord(o -> o.cancelTask(taskId2));
    cluster.callApi().waitForTaskToFinish(taskId2, overlord.latchableEmitter());

    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);
    assertRowCountEventually(batchA + batchB);
  }

  @Test
  public void test_restartedTask_immediateRestart_resumesFromLastAck() throws Exception
  {
    final String topic = dataSource + "_quick_restart_topic";
    kafkaServer.createTopicWithPartitions(topic, 1);
    kafkaServer.setShareGroupAutoOffsetReset(GROUP_ID + "-quick", "earliest");

    final int totalRecords = 20;
    kafkaServer.publishRecordsToTopic(topic, csvRecords(totalRecords, 0, "2025-08-01"));

    final String taskId1 = submitTaskWithGroup(topic, GROUP_ID + "-quick");
    Thread.sleep(SHARE_CONSUMER_READY_DELAY_MS);

    waitForRowsProcessed(totalRecords);

    cluster.callApi().onLeaderOverlord(o -> o.cancelTask(taskId1));
    cluster.callApi().waitForTaskToFinish(taskId1, overlord.latchableEmitter());

    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);
    assertRowCountEventually(totalRecords);

    // Immediately restart — produce 5 more records; the restarted task must NOT re-ingest the first 20.
    final String taskId2 = submitTaskWithGroup(topic, GROUP_ID + "-quick");
    Thread.sleep(SHARE_CONSUMER_READY_DELAY_MS);

    kafkaServer.publishRecordsToTopic(topic, csvRecords(5, totalRecords, "2025-08-02"));

    waitForRowsProcessed(totalRecords + 5);

    cluster.callApi().onLeaderOverlord(o -> o.cancelTask(taskId2));
    cluster.callApi().waitForTaskToFinish(taskId2, overlord.latchableEmitter());

    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);
    assertRowCountEventually(totalRecords + 5);
  }

  private String submitTask(String topic)
  {
    return submitTaskWithGroup(topic, GROUP_ID);
  }

  private String submitTaskWithGroup(String topic, String groupId)
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

  private void waitForRowsProcessed(long expected)
  {
    indexer.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("ingest/events/processed")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource),
        agg -> agg.hasSumAtLeast(expected)
    );
  }

  private void assertRowCountEventually(long expected)
  {
    final String expectedStr = String.valueOf(expected);
    final long deadlineMillis = System.currentTimeMillis() + 30_000L;
    String last = null;
    while (System.currentTimeMillis() < deadlineMillis) {
      try {
        last = cluster.runSql("SELECT COUNT(*) FROM %s", dataSource);
        if (expectedStr.equals(last)) {
          return;
        }
      }
      catch (Exception ignored) {
      }
      try {
        Thread.sleep(250L);
      }
      catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    Assertions.fail("Expected row count [" + expectedStr + "] but last result was [" + last + "]");
  }

  private List<byte[]> csvRecords(int count, int startIndex, String dateStr)
  {
    final List<byte[]> records = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      final String csv = dateStr + "T00:" + String.format("%02d", (startIndex + i) % 60) + ":00Z"
                         + ",item" + (startIndex + i) + "," + (startIndex + i);
      records.add(csv.getBytes(StandardCharsets.UTF_8));
    }
    return records;
  }
}
