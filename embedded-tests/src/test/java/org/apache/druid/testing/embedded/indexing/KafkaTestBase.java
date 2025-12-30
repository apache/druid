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

import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexing.kafka.KafkaIndexTaskModule;
import org.apache.druid.indexing.kafka.simulate.KafkaResource;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorSpecBuilder;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStatus;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.apache.druid.testing.tools.EventSerializer;
import org.apache.druid.testing.tools.JsonEventSerializer;
import org.apache.druid.testing.tools.StreamGenerator;
import org.apache.druid.testing.tools.WikipediaStreamEventStreamGenerator;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class KafkaTestBase extends EmbeddedClusterTestBase
{
  protected final KafkaResource kafkaServer = new KafkaResource();
  protected final EmbeddedOverlord overlord = new EmbeddedOverlord();
  protected final EmbeddedIndexer indexer = new EmbeddedIndexer();
  protected final EmbeddedBroker broker = new EmbeddedBroker();
  protected final EmbeddedHistorical historical = new EmbeddedHistorical();
  protected final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();

  protected int totalPublishedRecords = 0;
  protected String topic = null;
  protected SupervisorSpec supervisorSpec = null;

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    indexer.addProperty("druid.segment.handoff.pollDuration", "PT0.1s");

    return EmbeddedDruidCluster
        .withEmbeddedDerbyAndZookeeper()
        .useContainerFriendlyHostname()
        .addExtension(KafkaIndexTaskModule.class)
        .useLatchableEmitter()
        .useDefaultTimeoutForLatchableEmitter(60)
        .addResource(kafkaServer)
        .addServer(indexer)
        .addServer(coordinator)
        .addServer(overlord)
        .addServer(broker)
        .addServer(historical)
        .addServer(new EmbeddedRouter());
  }

  @BeforeEach
  public void initState()
  {
    topic = "topic_" + dataSource;
    supervisorSpec = null;
    totalPublishedRecords = 0;
  }

  protected KafkaSupervisorSpecBuilder createSupervisor()
  {
    return MoreResources.Supervisor.KAFKA_JSON
        .get()
        .withDataSchema(schema -> schema.withTimestamp(new TimestampSpec("timestamp", "iso", null)))
        .withTuningConfig(tuningConfig -> tuningConfig.withMaxRowsPerSegment(1))
        .withIoConfig(
            ioConfig -> ioConfig
                .withConsumerProperties(kafkaServer.consumerProperties())
                .withTaskCount(2)
        );
  }

  protected void setupTopicAndSupervisor(int partitionCount, boolean useTransactions)
  {
    kafkaServer.createTopicWithPartitions(topic, partitionCount);
    supervisorSpec = createSupervisor().withId("supe_" + dataSource).build(dataSource, topic);
    cluster.callApi().postSupervisor(supervisorSpec);

    publishRecords(topic, useTransactions);
    waitUntilPublishedRecordsAreIngested();
    verifySupervisorIsRunningHealthy();
  }

  protected void verifyDataAndTearDown(boolean useTransactions)
  {
    publishRecords(topic, useTransactions);
    waitUntilPublishedRecordsAreIngested();
    verifySupervisorIsRunningHealthy();

    cluster.callApi().postSupervisor(supervisorSpec.createSuspendedSpec());
    kafkaServer.deleteTopic(topic);

    verifyRowCount();
  }

  /**
   * Waits until number of processed events matches {@link #totalPublishedRecords}.
   */
  protected void waitUntilPublishedRecordsAreIngested()
  {
    indexer.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("ingest/events/processed")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource),
        agg -> agg.hasSumAtLeast(totalPublishedRecords)
    );

    final int totalEventsProcessed = indexer
        .latchableEmitter()
        .getMetricValues("ingest/events/processed", Map.of(DruidMetrics.DATASOURCE, dataSource))
        .stream()
        .mapToInt(Number::intValue)
        .sum();
    Assertions.assertEquals(totalPublishedRecords, totalEventsProcessed);
  }

  private void verifySupervisorIsRunningHealthy()
  {
    final SupervisorStatus status = cluster.callApi().getSupervisorStatus(supervisorSpec.getId());
    Assertions.assertTrue(status.isHealthy());
    Assertions.assertFalse(status.isSuspended());
    Assertions.assertEquals("RUNNING", status.getState());
  }

  /**
   * Verifies that the row count in {@link #dataSource} matches {@link #totalPublishedRecords}.
   */
  protected void verifyRowCount()
  {
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);
    cluster.callApi().verifySqlQuery(
        "SELECT COUNT(*) FROM %s",
        dataSource,
        String.valueOf(totalPublishedRecords)
    );
  }

  protected void publishRecords(String topic, boolean useTransactions)
  {
    totalPublishedRecords += generateRecordsAndPublishToKafka(topic, useTransactions);
  }

  protected int generateRecordsAndPublishToKafka(String topic, boolean useTransactions)
  {
    final EventSerializer serializer = new JsonEventSerializer(overlord.bindings().jsonMapper());
    final StreamGenerator streamGenerator = new WikipediaStreamEventStreamGenerator(serializer, 6, 100);
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
}
