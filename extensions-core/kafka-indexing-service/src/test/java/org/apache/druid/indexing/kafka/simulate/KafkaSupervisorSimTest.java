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

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.kafka.KafkaIndexTaskModule;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorIOConfig;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorSpec;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStatus;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.http.ClientSqlQuery;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.testing.simulate.EmbeddedBroker;
import org.apache.druid.testing.simulate.EmbeddedCoordinator;
import org.apache.druid.testing.simulate.EmbeddedDruidCluster;
import org.apache.druid.testing.simulate.EmbeddedIndexer;
import org.apache.druid.testing.simulate.EmbeddedOverlord;
import org.apache.druid.testing.simulate.junit5.DruidSimulationTestBase;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class KafkaSupervisorSimTest extends DruidSimulationTestBase
{
  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final EmbeddedIndexer indexer = EmbeddedIndexer.create();
  private final EmbeddedOverlord overlord = EmbeddedOverlord.create();
  private EmbeddedKafkaServer kafkaServer;

  @Override
  public EmbeddedDruidCluster createCluster()
  {
    final EmbeddedDruidCluster cluster = EmbeddedDruidCluster.withExtensions(List.of(KafkaIndexTaskModule.class));

    kafkaServer = new EmbeddedKafkaServer(cluster.getZookeeper(), cluster.getTestFolder(), Map.of());

    cluster.addResource(kafkaServer)
           .addServer(new EmbeddedCoordinator())
           .addServer(overlord)
           .addServer(indexer)
           .addServer(broker);

    return cluster;
  }

  @Test
  public void test_runKafkaTask() throws Exception
  {
    // Set up a topic
    final String topic = dataSource;
    kafkaServer.createTopicWithPartitions(topic, 2);

    // Produce some records to the topic
    final List<ProducerRecord<byte[], byte[]>> records = List.of(
        new ProducerRecord<>(topic, 0, null, StringUtils.toUtf8("2025-06-01,shirt,105")),
        new ProducerRecord<>(topic, 1, null, StringUtils.toUtf8("2025-06-02,trousers,210")),
        new ProducerRecord<>(topic, 0, null, StringUtils.toUtf8("2025-06-03,jeans,150")),
        new ProducerRecord<>(topic, 1, null, StringUtils.toUtf8("2025-06-04,t-shirt,53")),
        new ProducerRecord<>(topic, 0, null, StringUtils.toUtf8("2025-06-05,microwave,1099")),
        new ProducerRecord<>(topic, 1, null, StringUtils.toUtf8("2025-06-06,spoon,11")),
        new ProducerRecord<>(topic, 0, null, StringUtils.toUtf8("2025-06-07,television,1100")),
        new ProducerRecord<>(topic, 1, null, StringUtils.toUtf8("2025-06-08,plant pots,75")),
        new ProducerRecord<>(topic, 0, null, StringUtils.toUtf8("2025-06-09,shirt,99")),
        new ProducerRecord<>(topic, 1, null, StringUtils.toUtf8("2025-06-10,toys,101"))
    );
    kafkaServer.produceRecordsToTopic(records);

    // Submit and start a supervisor
    final String supervisorId = dataSource + "_supe";
    final KafkaSupervisorSpec kafkaSupervisorSpec = new KafkaSupervisorSpec(
        supervisorId,
        null,
        DataSchema.builder()
                  .withDataSource(dataSource)
                  .withTimestamp(new TimestampSpec("timestamp", null, null))
                  .withDimensions(DimensionsSpec.EMPTY)
                  .build(),
        null,
        new KafkaSupervisorIOConfig(
            topic,
            null,
            new CsvInputFormat(List.of("timestamp", "item"), null, null, false, 0, false),
            null, null,
            null,
            kafkaServer.consumerProperties(),
            null, null, null, null, null,
            true,
            null, null, null, null, null, null, null, null
        ),
        null, null, null, null, null, null, null, null, null, null, null
    );

    final Map<String, String> startSupervisorResult = getResult(
        cluster.leaderOverlord().postSupervisor(kafkaSupervisorSpec)
    );
    Assertions.assertEquals(Map.of("id", supervisorId), startSupervisorResult);

    // Wait for the broker to discover the realtime segments
    broker.serviceEmitter().waitForEvent(
        event -> event.hasDimension(DruidMetrics.DATASOURCE, dataSource)
    );

    SupervisorStatus supervisorStatus = getSupervisorStatus(supervisorId);
    Assertions.assertFalse(supervisorStatus.isSuspended());
    Assertions.assertTrue(supervisorStatus.isHealthy());
    Assertions.assertEquals(dataSource, supervisorStatus.getDataSource());
    Assertions.assertEquals("RUNNING", supervisorStatus.getState());
    Assertions.assertEquals(topic, supervisorStatus.getSource());

    // Get the task ID
    List<TaskStatusPlus> taskStatuses = ImmutableList.copyOf(
        getResult(cluster.leaderOverlord().taskStatuses(null, dataSource, 1))
    );
    Assertions.assertEquals(1, taskStatuses.size());
    Assertions.assertEquals(TaskState.RUNNING, taskStatuses.get(0).getStatusCode());

    // Verify the count of rows in the datasource so far
    final String queryResult = getResult(
        cluster.anyBroker().submitSqlQuery(
            new ClientSqlQuery(
                "SELECT COUNT(*) AS c FROM " + dataSource,
                ResultFormat.OBJECTLINES.name(),
                false,
                false,
                false,
                null,
                null
            )
        )
    );
    Map<String, Object> queryResultMap = TestHelper.JSON_MAPPER.readValue(
        queryResult,
        new TypeReference<>() {}
    );
    Assertions.assertEquals(Map.of("c", 10), queryResultMap);

    // Suspend the supervisor and verify the state
    getResult(
        cluster.leaderOverlord().postSupervisor(kafkaSupervisorSpec.createSuspendedSpec())
    );
    supervisorStatus = getSupervisorStatus(supervisorId);
    Assertions.assertTrue(supervisorStatus.isSuspended());
  }

  private SupervisorStatus getSupervisorStatus(String supervisorId)
  {
    final List<SupervisorStatus> supervisors = ImmutableList.copyOf(
        getResult(cluster.leaderOverlord().supervisorStatuses())
    );
    for (SupervisorStatus supervisor : supervisors) {
      if (supervisor.getId().equals(supervisorId)) {
        return supervisor;
      }
    }

    Assertions.fail("Could not find supervisor for id " + supervisorId);
    return null;
  }
}
