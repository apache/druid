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
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexing.kafka.KafkaIndexTaskModule;
import org.apache.druid.indexing.kafka.KafkaIndexTaskTuningConfig;
import org.apache.druid.indexing.kafka.ShareGroupIndexTask;
import org.apache.druid.indexing.kafka.ShareGroupIndexTaskIOConfig;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
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
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * End-to-end integration test for Kafka share group ingestion.
 *
 * Demonstrates the full workflow:
 * <ol>
 *   <li>Start an embedded Druid cluster with Kafka extension</li>
 *   <li>Create a Kafka topic and produce CSV records</li>
 *   <li>Submit a {@link ShareGroupIndexTask} via the Overlord API</li>
 *   <li>Wait for records to be ingested</li>
 *   <li>Verify data via SQL query</li>
 * </ol>
 *
 * This test requires a Kafka 4.0+ broker with share group support.
 * The KafkaResource Testcontainer uses apache/kafka:4.1.1 by default.
 */
public class EmbeddedShareGroupIngestionTest extends EmbeddedClusterTestBase
{
  private static final String COL_TIMESTAMP = "__time";
  private static final String COL_ITEM = "item";
  private static final String COL_VALUE = "value";

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
           .addServer(coordinator)
           .addServer(overlord)
           .addServer(indexer)
           .addServer(historical)
           .addServer(broker);

    return cluster;
  }

  /**
   * Showcase test: Submit a ShareGroupIndexTask, ingest data from Kafka
   * using share group semantics, and verify via SQL.
   */
  @Test
  public void test_shareGroupIngestion_basicEndToEnd()
  {
    final String topic = dataSource + "_topic";
    kafkaServer.createTopicWithPartitions(topic, 2);

    final int numRecords = 10;
    kafkaServer.produceRecordsToTopic(
        generateRecords(topic, numRecords, DateTimes.of("2025-06-01"))
    );

    // Build the task spec
    final DataSchema dataSchema = DataSchema.builder()
        .withDataSource(dataSource)
        .withTimestamp(new TimestampSpec(COL_TIMESTAMP, null, null))
        .withDimensions(DimensionsSpec.EMPTY)
        .withGranularity(new UniformGranularitySpec(
            org.apache.druid.java.util.common.granularity.Granularities.DAY,
            org.apache.druid.java.util.common.granularity.Granularities.NONE,
            null
        ))
        .build();

    final ShareGroupIndexTaskIOConfig ioConfig = new ShareGroupIndexTaskIOConfig(
        topic,
        "druid-share-group-test",
        kafkaServer.consumerProperties(),
        new CsvInputFormat(List.of(COL_TIMESTAMP, COL_ITEM, COL_VALUE), null, null, false, 0, false),
        null
    );

    final KafkaIndexTaskTuningConfig tuningConfig = new KafkaIndexTaskTuningConfig(
        null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null
    );

    final ObjectMapper mapper = cluster.callApi().objectMapper();
    final ShareGroupIndexTask task = new ShareGroupIndexTask(
        null,
        null,
        dataSchema,
        tuningConfig,
        ioConfig,
        null,
        mapper
    );

    // Submit the task
    cluster.callApi().submitTask(task);

    // Wait for records to be processed
    indexer.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("ingest/events/processed")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource),
        agg -> agg.hasSumAtLeast(numRecords)
    );

    // Gracefully stop the task so it publishes and exits
    // (In production, a supervisor would manage this lifecycle)

    // Verify ingested row count via SQL
    Assertions.assertEquals(
        String.valueOf(numRecords),
        cluster.runSql("SELECT COUNT(*) FROM %s", dataSource)
    );
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
}
