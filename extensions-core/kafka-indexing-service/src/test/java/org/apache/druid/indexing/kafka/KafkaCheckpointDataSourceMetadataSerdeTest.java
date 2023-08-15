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

package org.apache.druid.indexing.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.data.input.kafka.KafkaTopicPartition;
import org.apache.druid.indexing.common.actions.CheckPointDataSourceMetadataAction;
import org.apache.druid.indexing.seekablestream.SeekableStreamStartSequenceNumbers;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class KafkaCheckpointDataSourceMetadataSerdeTest
{
  private ObjectMapper objectMapper;

  @Before
  public void setUp()
  {
    objectMapper = new DefaultObjectMapper();
    objectMapper.registerSubtypes(KafkaDataSourceMetadata.class);
    objectMapper.registerSubtypes(KafkaTopicPartition.class);
    SimpleModule simpleModule = new SimpleModule();
    simpleModule.addKeySerializer(KafkaTopicPartition.class,
                                  new KafkaTopicPartition.KafkaTopicPartitionKeySerializer());
    objectMapper.registerModule(simpleModule);
  }

  @Test
  public void testCheckPointDataSourceMetadataActionSerde() throws IOException
  {
    final KafkaDataSourceMetadata kafkaDataSourceMetadata =
        new KafkaDataSourceMetadata(
            new SeekableStreamStartSequenceNumbers<>(
                "topic",
                ImmutableMap.of(
                    new KafkaTopicPartition(false, "topic", 0), 10L,
                    new KafkaTopicPartition(false, "topic", 1), 20L,
                    new KafkaTopicPartition(false, "topic", 2), 30L),
                ImmutableSet.of()
            )
        );
    final CheckPointDataSourceMetadataAction checkpointAction = new CheckPointDataSourceMetadataAction(
        "id_1",
        1,
        null,
        kafkaDataSourceMetadata
    );

    final String serialized = objectMapper.writeValueAsString(checkpointAction);
    final CheckPointDataSourceMetadataAction deserialized = objectMapper.readValue(
        serialized,
        CheckPointDataSourceMetadataAction.class
    );
    Assert.assertEquals(checkpointAction, deserialized);
  }

  @Test
  public void testMultiTopicCheckPointDataSourceMetadataActionSerde() throws IOException
  {
    final KafkaDataSourceMetadata kafkaDataSourceMetadata =
        new KafkaDataSourceMetadata(
            new SeekableStreamStartSequenceNumbers<>(
                "topic1,topic2",
                ImmutableMap.of(
                    new KafkaTopicPartition(true, "topic1", 0), 10L,
                    new KafkaTopicPartition(true, "topic1", 1), 20L,
                    new KafkaTopicPartition(true, "topic2", 0), 30L),
                ImmutableSet.of()
            )
        );
    final CheckPointDataSourceMetadataAction checkpointAction = new CheckPointDataSourceMetadataAction(
        "id_1",
        1,
        null,
        kafkaDataSourceMetadata
    );

    final String serialized = objectMapper.writeValueAsString(checkpointAction);
    final CheckPointDataSourceMetadataAction deserialized = objectMapper.readValue(
        serialized,
        CheckPointDataSourceMetadataAction.class
    );
    Assert.assertEquals(checkpointAction, deserialized);
  }

  @Test
  public void testCheckPointDataSourceMetadataActionOldJsonSerde() throws IOException
  {
    final String jsonStr = "{\n"
                           + "\t\"type\": \"checkPointDataSourceMetadata\",\n"
                           + "\t\"supervisorId\": \"id_1\",\n"
                           + "\t\"taskGroupId\": 1,\n"
                           + "\t\"previousCheckPoint\": {\n"
                           + "\t\t\"type\": \"KafkaDataSourceMetadata\",\n"
                           + "\t\t\"partitions\": {\n"
                           + "\t\t\t\"type\": \"start\",\n"
                           + "\t\t\t\"stream\": \"topic\",\n"
                           + "\t\t\t\"topic\": \"topic\",\n"
                           + "\t\t\t\"partitionSequenceNumberMap\": {\n"
                           + "\t\t\t\t\"0\": 10,\n"
                           + "\t\t\t\t\"1\": 20,\n"
                           + "\t\t\t\t\"2\": 30\n"
                           + "\t\t\t},\n"
                           + "\t\t\t\"partitionOffsetMap\": {\n"
                           + "\t\t\t\t\"0\": 10,\n"
                           + "\t\t\t\t\"1\": 20,\n"
                           + "\t\t\t\t\"2\": 30\n"
                           + "\t\t\t},\n"
                           + "\t\t\t\"exclusivePartitions\": []\n"
                           + "\t\t}\n"
                           + "\t},\n"
                           + "\t\"checkpointMetadata\": {\n"
                           + "\t\t\"type\": \"KafkaDataSourceMetadata\",\n"
                           + "\t\t\"partitions\": {\n"
                           + "\t\t\t\"type\": \"start\",\n"
                           + "\t\t\t\"stream\": \"topic\",\n"
                           + "\t\t\t\"topic\": \"topic\",\n"
                           + "\t\t\t\"partitionSequenceNumberMap\": {\n"
                           + "\t\t\t\t\"0\": 10,\n"
                           + "\t\t\t\t\"1\": 20,\n"
                           + "\t\t\t\t\"2\": 30\n"
                           + "\t\t\t},\n"
                           + "\t\t\t\"partitionOffsetMap\": {\n"
                           + "\t\t\t\t\"0\": 10,\n"
                           + "\t\t\t\t\"1\": 20,\n"
                           + "\t\t\t\t\"2\": 30\n"
                           + "\t\t\t},\n"
                           + "\t\t\t\"exclusivePartitions\": []\n"
                           + "\t\t}\n"
                           + "\t},\n"
                           + "\t\"currentCheckPoint\": {\n"
                           + "\t\t\"type\": \"KafkaDataSourceMetadata\",\n"
                           + "\t\t\"partitions\": {\n"
                           + "\t\t\t\"type\": \"start\",\n"
                           + "\t\t\t\"stream\": \"topic\",\n"
                           + "\t\t\t\"topic\": \"topic\",\n"
                           + "\t\t\t\"partitionSequenceNumberMap\": {\n"
                           + "\t\t\t\t\"0\": 10,\n"
                           + "\t\t\t\t\"1\": 20,\n"
                           + "\t\t\t\t\"2\": 30\n"
                           + "\t\t\t},\n"
                           + "\t\t\t\"partitionOffsetMap\": {\n"
                           + "\t\t\t\t\"0\": 10,\n"
                           + "\t\t\t\t\"1\": 20,\n"
                           + "\t\t\t\t\"2\": 30\n"
                           + "\t\t\t},\n"
                           + "\t\t\t\"exclusivePartitions\": []\n"
                           + "\t\t}\n"
                           + "\t},\n"
                           + "\t\"sequenceName\": \"dummy\"\n"
                           + "}";

    final CheckPointDataSourceMetadataAction actual = objectMapper.readValue(
        jsonStr,
        CheckPointDataSourceMetadataAction.class
    );

    KafkaDataSourceMetadata kafkaDataSourceMetadata =
        new KafkaDataSourceMetadata(
            new SeekableStreamStartSequenceNumbers<>(
                "topic",
                ImmutableMap.of(
                    new KafkaTopicPartition(false, null, 0), 10L,
                    new KafkaTopicPartition(false, null, 1), 20L,
                    new KafkaTopicPartition(false, null, 2), 30L),
                ImmutableSet.of()
            )
        );
    CheckPointDataSourceMetadataAction checkpointAction = new CheckPointDataSourceMetadataAction(
        "id_1",
        1,
        kafkaDataSourceMetadata,
        kafkaDataSourceMetadata
    );
    Assert.assertEquals(checkpointAction, actual);
  }
}
