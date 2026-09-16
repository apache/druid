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
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

public class ShareGroupIndexTaskIOConfigTest
{
  private ObjectMapper mapper;

  @BeforeEach
  public void setUp()
  {
    mapper = new DefaultObjectMapper();
    mapper.registerSubtypes(
        new NamedType(ShareGroupIndexTaskIOConfig.class, "kafka_share_group")
    );
  }

  @Test
  public void testSerdeWithAllFields() throws IOException
  {
    final Map<String, Object> consumerProps = ImmutableMap.of(
        "bootstrap.servers", "localhost:9092"
    );
    final ShareGroupIndexTaskIOConfig config = new ShareGroupIndexTaskIOConfig(
        "test-topic",
        "my-share-group",
        consumerProps,
        null,
        5000L
    );

    final String json = mapper.writeValueAsString(config);
    final ShareGroupIndexTaskIOConfig deserialized = mapper.readValue(json, ShareGroupIndexTaskIOConfig.class);

    Assertions.assertEquals("test-topic", deserialized.getTopic());
    Assertions.assertEquals("my-share-group", deserialized.getGroupId());
    Assertions.assertEquals(consumerProps, deserialized.getConsumerProperties());
    Assertions.assertNull(deserialized.getInputFormat());
    Assertions.assertEquals(5000L, deserialized.getPollTimeout());
  }

  @Test
  public void testSerdeWithDefaultPollTimeout() throws IOException
  {
    final Map<String, Object> consumerProps = ImmutableMap.of(
        "bootstrap.servers", "localhost:9092"
    );
    final ShareGroupIndexTaskIOConfig config = new ShareGroupIndexTaskIOConfig(
        "test-topic",
        "my-share-group",
        consumerProps,
        null,
        null
    );

    final String json = mapper.writeValueAsString(config);
    final ShareGroupIndexTaskIOConfig deserialized = mapper.readValue(json, ShareGroupIndexTaskIOConfig.class);

    Assertions.assertEquals("test-topic", deserialized.getTopic());
    Assertions.assertEquals("my-share-group", deserialized.getGroupId());
    // Default poll timeout from KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS
    Assertions.assertTrue(deserialized.getPollTimeout() > 0);
  }

  @Test
  public void testDeserializationFromJson() throws IOException
  {
    final String json = "{"
                        + "\"type\": \"kafka_share_group\","
                        + "\"topic\": \"events\","
                        + "\"groupId\": \"druid-share\","
                        + "\"consumerProperties\": {\"bootstrap.servers\": \"broker:9092\"},"
                        + "\"pollTimeout\": 2000"
                        + "}";

    final ShareGroupIndexTaskIOConfig config = mapper.readValue(json, ShareGroupIndexTaskIOConfig.class);
    Assertions.assertEquals("events", config.getTopic());
    Assertions.assertEquals("druid-share", config.getGroupId());
    Assertions.assertEquals("broker:9092", config.getConsumerProperties().get("bootstrap.servers"));
    Assertions.assertEquals(2000L, config.getPollTimeout());
  }

  @Test
  public void testTopicRequired()
  {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> new ShareGroupIndexTaskIOConfig(
            null,
            "my-share-group",
            ImmutableMap.of("bootstrap.servers", "localhost:9092"),
            null,
            null
        )
    );
  }

  @Test
  public void testGroupIdRequired()
  {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> new ShareGroupIndexTaskIOConfig(
            "test-topic",
            null,
            ImmutableMap.of("bootstrap.servers", "localhost:9092"),
            null,
            null
        )
    );
  }

  @Test
  public void testConsumerPropertiesRequired()
  {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> new ShareGroupIndexTaskIOConfig(
            "test-topic",
            "my-share-group",
            null,
            null,
            null
        )
    );
  }

  @Test
  public void testToString()
  {
    final ShareGroupIndexTaskIOConfig config = new ShareGroupIndexTaskIOConfig(
        "test-topic",
        "my-share-group",
        ImmutableMap.of("bootstrap.servers", "localhost:9092"),
        null,
        null
    );
    final String str = config.toString();
    Assertions.assertTrue(str.contains("test-topic"));
    Assertions.assertTrue(str.contains("my-share-group"));
  }
}
