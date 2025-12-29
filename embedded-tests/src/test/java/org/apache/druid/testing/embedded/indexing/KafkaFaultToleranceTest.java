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
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorSpec;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KafkaFaultToleranceTest extends EmbeddedClusterTestBase
{
  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedHistorical historical = new EmbeddedHistorical();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private final KafkaResource kafkaServer = new KafkaResource();

  private SupervisorSpec supervisorSpec;
  private String topic;
  private int totalPublishedRecords = 0;

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

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void test_kafkaSupervisor_recovers_afterOverlordRestart(boolean useTransactions) throws Exception
  {
    setupTopicAndSupervisor(2, useTransactions);
    overlord.stop();
    publishRecords(topic, useTransactions);
    overlord.start();
    verifyDataAndTearDown(useTransactions);
  }

  @Test
  public void test_kafkaSupervisor_recovers_afterCoordinatorRestart() throws Exception
  {
    final boolean useTransactions = true;
    setupTopicAndSupervisor(3, useTransactions);
    coordinator.stop();
    publishRecords(topic, useTransactions);
    coordinator.start();
    verifyDataAndTearDown(useTransactions);
  }

  @Test
  public void test_kafkaSupervisor_recovers_afterHistoricalRestart() throws Exception
  {
    final boolean useTransactions = false;
    setupTopicAndSupervisor(2, useTransactions);
    historical.stop();
    publishRecords(topic, useTransactions);
    historical.start();
    verifyDataAndTearDown(useTransactions);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void test_kafkaSupervisor_recovers_afterSuspendResume(boolean useTransactions)
  {
    setupTopicAndSupervisor(2, useTransactions);
    cluster.callApi().postSupervisor(supervisorSpec.createSuspendedSpec());
    publishRecords(topic, useTransactions);
    cluster.callApi().postSupervisor(supervisorSpec.createRunningSpec());
    verifyDataAndTearDown(useTransactions);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void test_kafkaSupervisor_recovers_afterChangeInTopicPartitions(boolean useTransactions)
  {
    setupTopicAndSupervisor(2, useTransactions);
    kafkaServer.increasePartitionsInTopic(topic, 4);
    publishRecords(topic, useTransactions);
    verifyDataAndTearDown(useTransactions);
  }

  private KafkaSupervisorSpec createKafkaSupervisor(String supervisorId, String topic)
  {
    return MoreResources.Supervisor.KAFKA_JSON
        .get()
        .withDataSchema(schema -> schema.withTimestamp(new TimestampSpec("timestamp", "iso", null)))
        .withTuningConfig(tuningConfig -> tuningConfig.withMaxRowsPerSegment(1))
        .withIoConfig(
            ioConfig -> ioConfig
                .withConsumerProperties(kafkaServer.consumerProperties())
                .withTaskCount(2)
        )
        .withId(supervisorId)
        .build(dataSource, topic);
  }

  private void setupTopicAndSupervisor(int partitionCount, boolean useTransactions)
  {
    kafkaServer.createTopicWithPartitions(dataSource, partitionCount);

    topic = "topic_" + dataSource;
    supervisorSpec = createKafkaSupervisor("supe_" + dataSource, topic);
    cluster.callApi().postSupervisor(supervisorSpec);

    totalPublishedRecords = 0;
    publishRecords(topic, useTransactions);
    verifyIngestedEvents();
  }

  private void verifyDataAndTearDown(boolean useTransactions)
  {
    publishRecords(topic, useTransactions);
    verifyIngestedEvents();

    cluster.callApi().postSupervisor(supervisorSpec.createSuspendedSpec());
    kafkaServer.deleteTopic(topic);
  }

  private void verifyIngestedEvents()
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

    final SupervisorStatus status = cluster.callApi().getSupervisorStatus(supervisorSpec.getId());
    Assertions.assertTrue(status.isHealthy());
    Assertions.assertFalse(status.isSuspended());
    Assertions.assertEquals("RUNNING", status.getState());
  }

  private void publishRecords(String topic, boolean useTransactions)
  {
    totalPublishedRecords += generateRecordsAndPublishToKafka(topic, useTransactions);
  }

  private int generateRecordsAndPublishToKafka(String topic, boolean useTransactions)
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
