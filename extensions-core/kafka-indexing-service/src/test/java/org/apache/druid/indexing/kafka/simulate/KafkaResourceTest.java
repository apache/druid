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

import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaResourceTest
{
  @Test
  @Timeout(60)
  public void testKafka()
  {
    final KafkaResource resource = new KafkaResource();

    resource.beforeStart(EmbeddedDruidCluster.empty());
    resource.start();
    assertTrue(resource.isRunning());

    // Verify bootstrap server URL
    final String bootstrapServerUrl = resource.getBootstrapServerUrl();
    assertTrue(bootstrapServerUrl.contains(":"));

    // Test producer properties
    final var producerProps = resource.producerProperties();
    assertEquals(bootstrapServerUrl, producerProps.get("bootstrap.servers"));
    assertEquals("all", producerProps.get("acks"));
    assertEquals("true", producerProps.get("enable.idempotence"));

    // Test consumer properties
    final Map<String, Object> consumerProps = resource.consumerProperties();
    assertEquals(bootstrapServerUrl, consumerProps.get("bootstrap.servers"));

    // Test topic creation
    final String topicName = "test-topic";
    resource.createTopicWithPartitions(topicName, 3);
    assertEquals(Set.of(topicName), resource.listTopics());

    // Verify that every partition can accept records immediately after creating a topic.
    resource.produceRecordsWithoutTransaction(
        List.of(
            new ProducerRecord<>(topicName, 0, null, new byte[]{1}),
            new ProducerRecord<>(topicName, 1, null, new byte[]{1}),
            new ProducerRecord<>(topicName, 2, null, new byte[]{1})
        )
    );
    assertEquals(
        Map.of("0", 1L, "1", 1L, "2", 1L),
        resource.getPartitionOffsets(topicName)
    );
    resource.deleteTopic(topicName);

    final String expandedTopicName = "test-expanded-topic";
    resource.createTopicWithPartitions(expandedTopicName, 2);
    resource.increasePartitionsInTopic(expandedTopicName, 4);

    // Verify that every partition can accept records immediately after increasing the partition count.
    resource.produceRecordsWithoutTransaction(
        List.of(
            new ProducerRecord<>(expandedTopicName, 0, null, new byte[]{1}),
            new ProducerRecord<>(expandedTopicName, 1, null, new byte[]{1}),
            new ProducerRecord<>(expandedTopicName, 2, null, new byte[]{1}),
            new ProducerRecord<>(expandedTopicName, 3, null, new byte[]{1})
        )
    );
    assertEquals(
        Map.of("0", 1L, "1", 1L, "2", 1L, "3", 1L),
        resource.getPartitionOffsets(expandedTopicName)
    );
    resource.deleteTopic(expandedTopicName);

    resource.stop();
    assertFalse(resource.isRunning());
  }
}
