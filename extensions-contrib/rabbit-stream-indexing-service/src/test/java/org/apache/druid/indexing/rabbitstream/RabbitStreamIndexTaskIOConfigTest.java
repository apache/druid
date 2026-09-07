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

package org.apache.druid.indexing.rabbitstream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.indexing.seekablestream.SeekableStreamStartSequenceNumbers;
import org.apache.druid.indexing.seekablestream.supervisor.BoundedStreamConfig;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.segment.indexing.IOConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

public class RabbitStreamIndexTaskIOConfigTest
{
  private final ObjectMapper mapper;

  public RabbitStreamIndexTaskIOConfigTest()
  {
    mapper = new DefaultObjectMapper();
    mapper.registerModules(new RabbitStreamIndexTaskModule().getJacksonModules());
  }

  @Test
  public void testSerdeWithDefaults() throws Exception
  {
    String jsonStr = "{\n"
        + "  \"type\": \"rabbit\",\n"
        + "  \"baseSequenceName\": \"my-sequence-name\",\n"
        + "  \"uri\": \"rabbitmq-stream://localhost:5552\",\n"
        + "  \"startSequenceNumbers\": {\"type\":\"start\", \"stream\":\"mystream\", \"partitionSequenceNumberMap\" : {\"stream-0\":1, \"stream-1\":10}},\n"
        + "  \"endSequenceNumbers\": {\"type\":\"end\", \"stream\":\"mystream\", \"partitionSequenceNumberMap\" : {\"stream-0\":15, \"stream-1\":200}}\n"
        + "}";

    RabbitStreamIndexTaskIOConfig config = (RabbitStreamIndexTaskIOConfig) mapper.readValue(
        mapper.writeValueAsString(mapper.readValue(jsonStr, IOConfig.class)),
        IOConfig.class
    );

    Assertions.assertNull(config.getTaskGroupId());
    Assertions.assertEquals("my-sequence-name", config.getBaseSequenceName());

    Assertions.assertEquals("mystream", config.getStartSequenceNumbers().getStream());

    Assertions.assertEquals(
        ImmutableMap.of("stream-0", 1L, "stream-1", 10L),
        config.getStartSequenceNumbers().getPartitionSequenceNumberMap()
    );

    Assertions.assertEquals("mystream", config.getEndSequenceNumbers().getStream());

    Assertions.assertEquals(
        ImmutableMap.of("stream-0", 15L, "stream-1", 200L),
        config.getEndSequenceNumbers().getPartitionSequenceNumberMap()
    );

    Assertions.assertTrue(config.isUseTransaction());
    Assertions.assertNull(config.getMinimumMessageTime(), "minimumMessageTime");
    Assertions.assertEquals(config.getUri(), "rabbitmq-stream://localhost:5552");
    Assertions.assertEquals(Collections.emptySet(), config.getStartSequenceNumbers().getExclusivePartitions());
  }

  @Test
  public void testRabbitStreamDataSourceMetadataWithBoundedConfig()
  {
    Map<String, Long> startOffsets = ImmutableMap.of("q0", 0L);
    Map<String, Long> endOffsets = ImmutableMap.of("q0", 100L);
    BoundedStreamConfig boundedConfig = new BoundedStreamConfig(startOffsets, endOffsets);

    SeekableStreamStartSequenceNumbers<String, Long> partitions =
        new SeekableStreamStartSequenceNumbers<>("stream", ImmutableMap.of("q0", 10L), Collections.emptySet());

    RabbitStreamDataSourceMetadata metadata = new RabbitStreamDataSourceMetadata(partitions, boundedConfig);
    Assertions.assertNotNull(metadata.getBoundedStreamConfig());
    Assertions.assertEquals(boundedConfig, metadata.getBoundedStreamConfig());
  }

  @Test
  public void testRabbitStreamDataSourceMetadataWithoutBoundedConfig()
  {
    SeekableStreamStartSequenceNumbers<String, Long> partitions =
        new SeekableStreamStartSequenceNumbers<>("stream", ImmutableMap.of("q0", 10L), Collections.emptySet());

    RabbitStreamDataSourceMetadata metadata = new RabbitStreamDataSourceMetadata(partitions);
    Assertions.assertNull(metadata.getBoundedStreamConfig());
  }

}
