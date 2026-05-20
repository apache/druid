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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.base.Throwables;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Verifies that ShareGroupIndexTask can be submitted via raw JSON through the Overlord HTTP API. */
public class ShareGroupIndexTaskJsonSubmitIT extends EmbeddedClusterTestBase
{
  private static final long SHARE_CONSUMER_READY_DELAY_MS = 3_000L;
  private static final String COL_TIMESTAMP = "__time";
  private static final String COL_ITEM = "item";
  private static final String COL_VALUE = "value";

  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer();
  private final EmbeddedHistorical historical = new EmbeddedHistorical();
  private final EmbeddedBroker broker = new EmbeddedBroker();

  private ShareGroupKafkaResource kafkaServer;
  private final ObjectMapper mapper = createMapper();

  private static ObjectMapper createMapper()
  {
    final ObjectMapper m = new DefaultObjectMapper();
    m.registerSubtypes(
        new NamedType(ShareGroupIndexTask.class, "index_kafka_share_group"),
        new NamedType(ShareGroupIndexTaskIOConfig.class, "kafka_share_group"),
        new NamedType(KafkaIndexTaskTuningConfig.class, "KafkaTuningConfig")
    );
    return m;
  }

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
  public void test_jsonSubmit_csvFormat_ingestsData() throws Exception
  {
    final String topic = dataSource + "_topic";
    kafkaServer.createTopicWithPartitions(topic, 2);
    kafkaServer.setShareGroupAutoOffsetReset("json-submit-group", "earliest");

    final ShareGroupIndexTask task = buildTask(topic, "json-submit-group");
    final JsonNode taskJson = mapper.readTree(mapper.writeValueAsString(task));
    cluster.callApi().onLeaderOverlord(o -> o.runTask(task.getId(), taskJson));

    Thread.sleep(SHARE_CONSUMER_READY_DELAY_MS);

    final int numRecords = 8;
    kafkaServer.publishRecordsToTopic(topic, generateRecords(numRecords));

    indexer.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("ingest/events/processed")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource),
        agg -> agg.hasSumAtLeast(numRecords)
    );
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);

    assertRowCountEventually(numRecords);

    cluster.callApi().onLeaderOverlord(o -> o.cancelTask(task.getId()));
    cluster.callApi().waitForTaskToFinish(task.getId(), overlord.latchableEmitter());
  }

  @Test
  public void test_jsonSubmit_overlordReturnsExpectedTaskType() throws Exception
  {
    final String topic = dataSource + "_roundtrip_topic";
    kafkaServer.createTopicWithPartitions(topic, 1);
    kafkaServer.setShareGroupAutoOffsetReset("roundtrip-group", "earliest");

    final ShareGroupIndexTask task = buildTask(topic, "roundtrip-group");
    final JsonNode taskJson = mapper.readTree(mapper.writeValueAsString(task));
    cluster.callApi().onLeaderOverlord(o -> o.runTask(task.getId(), taskJson));

    Thread.sleep(SHARE_CONSUMER_READY_DELAY_MS);

    final org.apache.druid.indexer.TaskStatusResponse statusResponse =
        cluster.callApi().onLeaderOverlord(o -> o.taskStatus(task.getId()));
    Assertions.assertNotNull(statusResponse, "Overlord did not return a status for the submitted task");
    Assertions.assertNotNull(statusResponse.getStatus(), "Overlord status payload was empty");
    Assertions.assertEquals(task.getId(), statusResponse.getStatus().getId());
    Assertions.assertEquals("index_kafka_share_group", statusResponse.getStatus().getType());

    cluster.callApi().onLeaderOverlord(o -> o.cancelTask(task.getId()));
    cluster.callApi().waitForTaskToFinish(task.getId(), overlord.latchableEmitter());
  }

  @Test
  public void test_jsonSubmit_missingRequiredField_isRejected()
  {
    final String malformedJson =
        "{\"type\":\"index_kafka_share_group\","
        + "\"dataSchema\":{\"dataSource\":\"" + dataSource + "\","
        + "\"timestampSpec\":{\"column\":\"__time\",\"format\":\"auto\"},"
        + "\"dimensionsSpec\":{}},"
        + "\"ioConfig\":{\"type\":\"kafka_share_group\"}}";

    final Exception ex = Assertions.assertThrows(
        Exception.class,
        () -> {
          final JsonNode node = mapper.readTree(malformedJson);
          cluster.callApi().onLeaderOverlord(o -> o.runTask("bad-task-id", node));
        }
    );
    final Throwable rootCause = Throwables.getRootCause(ex);
    Assertions.assertNotNull(rootCause);
  }

  private ShareGroupIndexTask buildTask(String topic, String groupId)
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
                                            .withDimensions(DimensionsSpec.builder()
                                                                          .setDimensions(
                                                                              DimensionsSpec.getDefaultSchemas(
                                                                                  List.of(COL_ITEM, COL_VALUE)
                                                                              )
                                                                          )
                                                                          .build())
                                            .withGranularity(
                                                new UniformGranularitySpec(
                                                    Granularities.DAY,
                                                    Granularities.NONE,
                                                    null
                                                )
                                            )
                                            .build();
    return new ShareGroupIndexTask(
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

  private List<byte[]> generateRecords(int count)
  {
    final List<byte[]> records = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      final String csv = "2025-06-01T00:0" + (i % 6) + ":00Z,item" + i + "," + i;
      records.add(csv.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }
    return records;
  }
}
