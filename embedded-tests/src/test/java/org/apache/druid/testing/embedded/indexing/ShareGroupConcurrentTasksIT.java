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

public class ShareGroupConcurrentTasksIT extends EmbeddedClusterTestBase
{
  private static final String COL_TIMESTAMP = "__time";
  private static final String COL_ITEM = "item";
  private static final String COL_VALUE = "value";
  private static final String GROUP_ID = "concurrent-it-group";
  private static final int TOTAL_RECORDS = 50;

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
    indexer.addProperty("druid.worker.capacity", "4");
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
  public void test_twoConcurrentTasks_sameGroupId_noRowLoss() throws Exception
  {
    final String topic = dataSource + "_concurrent_topic";
    kafkaServer.createTopicWithPartitions(topic, 4);
    kafkaServer.setShareGroupAutoOffsetReset(GROUP_ID, "earliest");

    kafkaServer.publishRecordsToTopic(topic, csvRecords(TOTAL_RECORDS, 0, "2026-01-01"));

    final String taskA = submitTaskWithGroup(topic, GROUP_ID);
    final String taskB = submitTaskWithGroup(topic, GROUP_ID);

    Thread.sleep(15_000L);
    cluster.callApi().onLeaderOverlord(o -> o.cancelTask(taskA));
    cluster.callApi().onLeaderOverlord(o -> o.cancelTask(taskB));
    cluster.callApi().waitForTaskToFinish(taskA, overlord.latchableEmitter());
    cluster.callApi().waitForTaskToFinish(taskB, overlord.latchableEmitter());
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);

    assertDistinctRowCountAtLeast(TOTAL_RECORDS);
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

  private void assertDistinctRowCountAtLeast(long expected)
  {
    final long deadlineMillis = System.currentTimeMillis() + 30_000L;
    String last = null;
    while (System.currentTimeMillis() < deadlineMillis) {
      try {
        last = cluster.runSql("SELECT COUNT(DISTINCT %s) FROM %s", COL_ITEM, dataSource);
        if (last != null && !last.isEmpty() && Long.parseLong(last.trim()) >= expected) {
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
    Assertions.fail(
        "Expected at least [" + expected + "] distinct rows after concurrent share-group ingestion, but last result was [" + last + "]"
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
