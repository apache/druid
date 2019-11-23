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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.indexing.common.actions.CheckPointDataSourceMetadataAction;
import org.apache.druid.indexing.seekablestream.SeekableStreamStartSequenceNumbers;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class KafkaCheckpointDataSourceMetadataSerdeTest
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();

  @Test
  public void testCheckPointDataSourceMetadataActionSerde() throws IOException
  {
    MAPPER.registerSubtypes(KafkaDataSourceMetadata.class);

    final KafkaDataSourceMetadata kafkaDataSourceMetadata =
        new KafkaDataSourceMetadata(
            new SeekableStreamStartSequenceNumbers<>(
                "topic",
                ImmutableMap.of(0, 10L, 1, 20L, 2, 30L),
                ImmutableSet.of()
            )
        );
    final CheckPointDataSourceMetadataAction checkpointAction = new CheckPointDataSourceMetadataAction(
        "id_1",
        1,
        null,
        kafkaDataSourceMetadata
    );

    final String serialized = MAPPER.writeValueAsString(checkpointAction);
    final CheckPointDataSourceMetadataAction deserialized = MAPPER.readValue(
        serialized,
        CheckPointDataSourceMetadataAction.class
    );
    Assert.assertEquals(checkpointAction, deserialized);
  }

  @Test
  public void testCheckPointDataSourceMetadataActionOldJsonSerde() throws IOException
  {
    MAPPER.registerSubtypes(KafkaDataSourceMetadata.class);
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

    final CheckPointDataSourceMetadataAction actual = MAPPER.readValue(
        jsonStr,
        CheckPointDataSourceMetadataAction.class
    );

    KafkaDataSourceMetadata kafkaDataSourceMetadata =
        new KafkaDataSourceMetadata(
            new SeekableStreamStartSequenceNumbers<>(
                "topic",
                ImmutableMap.of(0, 10L, 1, 20L, 2, 30L),
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
