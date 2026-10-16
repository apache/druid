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
import org.apache.druid.storage.local.LocalFileStorageConnectorProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
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
        new NamedType(ShareGroupIndexTaskIOConfig.class, "kafka_share_group"),
        new NamedType(LocalFileStorageConnectorProvider.class, LocalFileStorageConnectorProvider.TYPE_NAME)
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
        5000L,
        "generation-1",
        new LocalFileStorageConnectorProvider(new File("/tmp/share-inbox"))
    );

    final String json = mapper.writeValueAsString(config);
    final ShareGroupIndexTaskIOConfig deserialized = mapper.readValue(json, ShareGroupIndexTaskIOConfig.class);

    Assertions.assertEquals("test-topic", deserialized.getTopic());
    Assertions.assertEquals("my-share-group", deserialized.getGroupId());
    Assertions.assertEquals(consumerProps, deserialized.getConsumerProperties());
    Assertions.assertNull(deserialized.getInputFormat());
    Assertions.assertEquals(5000L, deserialized.getPollTimeout());
    Assertions.assertEquals("generation-1", deserialized.getInboxId());
    Assertions.assertEquals(config.getInboxStorage(), deserialized.getInboxStorage());
    Assertions.assertEquals(4_096, deserialized.getReceiptPageSize());
    Assertions.assertEquals(10_000, deserialized.getMaxStagingRecords());
    Assertions.assertEquals(256L << 20, deserialized.getMaxStagingBytes());
    Assertions.assertEquals(2, deserialized.getMaxConcurrentUploads());
    Assertions.assertEquals(0.5, deserialized.getRenewalFraction());
    Assertions.assertEquals(16, deserialized.getMaxProcessingManifests());
    Assertions.assertEquals(100_000, deserialized.getMaxProcessingRecords());
    Assertions.assertEquals(256L << 20, deserialized.getMaxProcessingBytes());
    Assertions.assertEquals(300_000L, deserialized.getClaimDurationMillis());
    Assertions.assertEquals(60_000L, deserialized.getClaimRenewalPeriodMillis());
    Assertions.assertEquals(1_000L, deserialized.getInboxPollPeriodMillis());
  }

  @Test
  public void testSerdeDurableInboxFields() throws IOException
  {
    final LocalFileStorageConnectorProvider storage = new LocalFileStorageConnectorProvider(
        new File("/tmp/share-inbox")
    );
    final ShareGroupIndexTaskIOConfig config = new ShareGroupIndexTaskIOConfig(
        "test-topic",
        "my-share-group",
        ImmutableMap.of("bootstrap.servers", "localhost:9092"),
        null,
        5000L,
        "generation-1",
        storage,
        8_192,
        20_000,
        512L << 20,
        4,
        0.4,
        24,
        200_000,
        768L << 20,
        600_000L,
        120_000L,
        2_000L
    );

    final ShareGroupIndexTaskIOConfig deserialized = mapper.readValue(
        mapper.writeValueAsString(config),
        ShareGroupIndexTaskIOConfig.class
    );

    Assertions.assertEquals("generation-1", deserialized.getInboxId());
    Assertions.assertEquals(storage, deserialized.getInboxStorage());
    Assertions.assertEquals(8_192, deserialized.getReceiptPageSize());
    Assertions.assertEquals(20_000, deserialized.getMaxStagingRecords());
    Assertions.assertEquals(512L << 20, deserialized.getMaxStagingBytes());
    Assertions.assertEquals(4, deserialized.getMaxConcurrentUploads());
    Assertions.assertEquals(0.4, deserialized.getRenewalFraction());
    Assertions.assertEquals(24, deserialized.getMaxProcessingManifests());
    Assertions.assertEquals(200_000, deserialized.getMaxProcessingRecords());
    Assertions.assertEquals(768L << 20, deserialized.getMaxProcessingBytes());
    Assertions.assertEquals(600_000L, deserialized.getClaimDurationMillis());
    Assertions.assertEquals(120_000L, deserialized.getClaimRenewalPeriodMillis());
    Assertions.assertEquals(2_000L, deserialized.getInboxPollPeriodMillis());
  }

  @Test
  public void testRequiresInboxIdentityAndStorage()
  {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> durableConfig(null, new LocalFileStorageConnectorProvider(new File("/tmp/share-inbox")))
    );
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> durableConfig("", new LocalFileStorageConnectorProvider(new File("/tmp/share-inbox")))
    );
    Assertions.assertThrows(NullPointerException.class, () -> durableConfig("generation-1", null));
  }

  @Test
  public void testRejectsInvalidBounds()
  {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new ShareGroupIndexTaskIOConfig(
            "test-topic",
            "my-share-group",
            ImmutableMap.of("bootstrap.servers", "localhost:9092"),
            null,
            5_000L,
            "generation-1",
            new LocalFileStorageConnectorProvider(new File("/tmp/share-inbox")),
            0,
            1,
            1L,
            1,
            0.5,
            null,
            null,
            null,
            null,
            null,
            null
        )
    );
  }

  @Test
  public void testRejectsClaimRenewalAtOrAfterExpiration()
  {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new ShareGroupIndexTaskIOConfig(
            "test-topic",
            "my-share-group",
            ImmutableMap.of("bootstrap.servers", "localhost:9092"),
            null,
            5_000L,
            "generation-1",
            new LocalFileStorageConnectorProvider(new File("/tmp/share-inbox")),
            4_096,
            10_000,
            256L << 20,
            2,
            0.5,
            16,
            100_000,
            256L << 20,
            60_000L,
            60_000L,
            1_000L
        )
    );
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
        null,
        "generation-1",
        new LocalFileStorageConnectorProvider(new File("/tmp/share-inbox"))
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
                        + "\"pollTimeout\": 2000,"
                        + "\"inboxId\": \"generation-1\","
                        + "\"inboxStorage\": {\"type\": \"local\", \"basePath\": \"/tmp/share-inbox\"}"
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
            null,
            "generation-1",
            new LocalFileStorageConnectorProvider(new File("/tmp/share-inbox"))
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
            null,
            "generation-1",
            new LocalFileStorageConnectorProvider(new File("/tmp/share-inbox"))
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
            null,
            "generation-1",
            new LocalFileStorageConnectorProvider(new File("/tmp/share-inbox"))
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
        null,
        "generation-1",
        new LocalFileStorageConnectorProvider(new File("/tmp/share-inbox"))
    );
    final String str = config.toString();
    Assertions.assertTrue(str.contains("test-topic"));
    Assertions.assertTrue(str.contains("my-share-group"));
  }

  private static ShareGroupIndexTaskIOConfig durableConfig(
      String inboxId,
      LocalFileStorageConnectorProvider storage
  )
  {
    return new ShareGroupIndexTaskIOConfig(
        "test-topic",
        "my-share-group",
        ImmutableMap.of("bootstrap.servers", "localhost:9092"),
        null,
        5_000L,
        inboxId,
        storage
    );
  }
}
