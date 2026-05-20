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
 * Verifies that the Kafka broker automatically rebalances new partitions to share-group consumers
 * without any Druid-side intervention (no supervisor, no task restart).
 */
public class ShareGroupPartitionRebalancingIT extends EmbeddedClusterTestBase
{
  private static final long SHARE_CONSUMER_READY_DELAY_MS = 3_000L;
  private static final long PARTITION_REBALANCE_DELAY_MS = 2_000L;
  private static final String COL_TIMESTAMP = "__time";
  private static final String COL_ITEM = "item";
  private static final String COL_VALUE = "value";

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
  public void test_singleTask_partitionsIncrease_newPartitionsAutomaticallyConsumed() throws Exception
  {
    final String topic = dataSource + "_rebalance_single_topic";
    final String groupId = "rebalance-single-group";
    kafkaServer.createTopicWithPartitions(topic, 2);
    kafkaServer.setShareGroupAutoOffsetReset(groupId, "earliest");

    final String taskId = submitTask(topic, groupId);
    Thread.sleep(SHARE_CONSUMER_READY_DELAY_MS);

    final int batchA = 10;
    kafkaServer.publishRecordsToTopic(topic, csvRecords(batchA, 0, "2025-11-01"));

    waitForRowsProcessed(batchA);

    // Increase from 2 to 4 partitions; broker rebalances automatically.
    kafkaServer.increasePartitionsInTopic(topic, 4);
    Thread.sleep(PARTITION_REBALANCE_DELAY_MS);

    final int batchB = 10;
    kafkaServer.publishRecordsToTopic(topic, csvRecords(batchB, batchA, "2025-11-02"));

    waitForRowsProcessed(batchA + batchB);

    cluster.callApi().onLeaderOverlord(o -> o.cancelTask(taskId));
    cluster.callApi().waitForTaskToFinish(taskId, overlord.latchableEmitter());

    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);

    final long rowCount = Long.parseLong(cluster.runSql("SELECT COUNT(*) FROM %s", dataSource));
    Assertions.assertTrue(
        rowCount >= batchA + batchB,
        "Expected at least [" + (batchA + batchB) + "] rows but got [" + rowCount + "]"
    );
  }

  @Test
  public void test_multiTask_partitionsIncrease_brokerDistributesNewPartitions() throws Exception
  {
    final String topic = dataSource + "_rebalance_multi_topic";
    final String groupId = "rebalance-multi-group";
    kafkaServer.createTopicWithPartitions(topic, 2);
    kafkaServer.setShareGroupAutoOffsetReset(groupId, "earliest");

    final String taskId1 = submitTask(topic, groupId);
    final String taskId2 = submitTask(topic, groupId);
    Thread.sleep(SHARE_CONSUMER_READY_DELAY_MS);

    final int batchA = 10;
    kafkaServer.publishRecordsToTopic(topic, csvRecords(batchA, 0, "2025-12-01"));

    waitForRowsProcessed(batchA);

    // Increase from 2 to 4 partitions; broker distributes the 2 new partitions across both consumers.
    kafkaServer.increasePartitionsInTopic(topic, 4);
    Thread.sleep(PARTITION_REBALANCE_DELAY_MS);

    final int batchB = 20;
    kafkaServer.publishRecordsToTopic(topic, csvRecords(batchB, batchA, "2025-12-02"));

    waitForRowsProcessed(batchA + batchB);

    cluster.callApi().onLeaderOverlord(o -> o.cancelTask(taskId1));
    cluster.callApi().onLeaderOverlord(o -> o.cancelTask(taskId2));
    cluster.callApi().waitForTaskToFinish(taskId1, overlord.latchableEmitter());
    cluster.callApi().waitForTaskToFinish(taskId2, overlord.latchableEmitter());

    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);

    final long rowCount = Long.parseLong(cluster.runSql("SELECT COUNT(*) FROM %s", dataSource));
    Assertions.assertTrue(
        rowCount >= batchA + batchB,
        "Expected at least [" + (batchA + batchB) + "] rows but got [" + rowCount + "]"
    );
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

  private void waitForRowsProcessed(long expected)
  {
    indexer.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("ingest/events/processed")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource),
        agg -> agg.hasSumAtLeast(expected)
    );
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
