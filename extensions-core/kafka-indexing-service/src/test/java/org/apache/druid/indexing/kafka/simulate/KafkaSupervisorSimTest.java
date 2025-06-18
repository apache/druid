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

import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexing.kafka.KafkaIndexTaskModule;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorIOConfig;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorSpec;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStatus;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.testing.simulate.EmbeddedCoordinator;
import org.apache.druid.testing.simulate.EmbeddedDruidCluster;
import org.apache.druid.testing.simulate.EmbeddedIndexer;
import org.apache.druid.testing.simulate.EmbeddedOverlord;
import org.apache.druid.testing.simulate.junit5.DruidSimulationTestBase;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class KafkaSupervisorSimTest extends DruidSimulationTestBase
{
  private EmbeddedKafkaServer kafkaServer;
  private EmbeddedDruidCluster cluster;

  @Override
  public EmbeddedDruidCluster createCluster()
  {
    cluster = EmbeddedDruidCluster.withExtensions(List.of(KafkaIndexTaskModule.class))
                                  .addServer(new EmbeddedCoordinator())
                                  .addServer(EmbeddedIndexer.create())
                                  .addServer(EmbeddedOverlord.create());

    kafkaServer = new EmbeddedKafkaServer(cluster.getZookeeper(), cluster.getTestFolder(), Map.of());
    cluster.addResource(kafkaServer);

    return cluster;
  }

  @Test
  public void test_runKafkaTask() throws Exception
  {
    // Set up a topic
    final String topic = TestDataSource.WIKI;
    kafkaServer.createTopicWithPartitions(topic, 2);

    // Produce some records to the topic
    final List<ProducerRecord<byte[], byte[]>> records = List.of(
        new ProducerRecord<>(topic, 0, StringUtils.toUtf8("key1"), StringUtils.toUtf8("value1")),
        new ProducerRecord<>(topic, 1, StringUtils.toUtf8("key2"), StringUtils.toUtf8("value2"))
    );
    kafkaServer.produceRecordsToTopic(records);

    // Submit and start a supervisor
    final KafkaSupervisorSpec kafkaSupervisorSpec = new KafkaSupervisorSpec(
        null,
        null,
        DataSchema.builder()
                  .withDataSource(TestDataSource.WIKI)
                  .withTimestamp(new TimestampSpec("time", null, null))
                  .withDimensions(DimensionsSpec.EMPTY)
                  .build(),
        null,
        new KafkaSupervisorIOConfig(
            topic,
            null,
            new CsvInputFormat(List.of("col1"), null, null, false, 0, false),
            null, null, null,
            kafkaServer.consumerProperties(),
            null, null, null, null, null,
            true,
            null, null, null, null, null, null, null,
            false
        ),
        Map.of(),
        false,
        null, null, null, null, null, null, null, null, null
    );

    final Map<String, String> startSupervisorResult = getResult(
        cluster.leaderOverlord().postSupervisor(kafkaSupervisorSpec)
    );

    CloseableIterator<SupervisorStatus> supervisorStatuses = getResult(
        cluster.leaderOverlord().supervisorStatuses()
    );

    // Get the task ID
    // Let that task finish
    // Verify that there are some segments

    // Suspend supervisor
    final Map<String, String> suspendSupervisorResult = getResult(
        cluster.leaderOverlord().postSupervisor(kafkaSupervisorSpec.createSuspendedSpec())
    );
  }
}
