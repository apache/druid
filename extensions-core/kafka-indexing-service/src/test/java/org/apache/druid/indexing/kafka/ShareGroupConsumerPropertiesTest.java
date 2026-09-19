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

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

public class ShareGroupConsumerPropertiesTest
{
  @Test
  public void testSanitizeKeepsSupportedProperties()
  {
    final Map<String, Object> input = ImmutableMap.of(
        "bootstrap.servers", "broker:9092",
        "max.poll.records", 100,
        "ssl.protocol", "TLSv1.3"
    );
    final Map<String, Object> sanitized = ShareGroupConsumerProperties.sanitize(input);
    Assertions.assertEquals(input, sanitized);
  }

  @Test
  public void testSanitizeStripsAutoOffsetReset()
  {
    final Map<String, Object> input = ImmutableMap.of(
        "bootstrap.servers", "broker:9092",
        "auto.offset.reset", "earliest"
    );
    final Map<String, Object> sanitized = ShareGroupConsumerProperties.sanitize(input);
    Assertions.assertEquals(1, sanitized.size());
    Assertions.assertEquals("broker:9092", sanitized.get("bootstrap.servers"));
    Assertions.assertFalse(sanitized.containsKey("auto.offset.reset"));
  }

  @Test
  public void testSanitizeStripsAllUnsupportedKeys()
  {
    final Map<String, Object> input = new LinkedHashMap<>();
    input.put("bootstrap.servers", "broker:9092");
    for (String key : ShareGroupConsumerProperties.UNSUPPORTED_CONFIGS) {
      input.put(key, "some-value");
    }
    final Map<String, Object> sanitized = ShareGroupConsumerProperties.sanitize(input);
    Assertions.assertEquals(1, sanitized.size());
    Assertions.assertEquals("broker:9092", sanitized.get("bootstrap.servers"));
  }

  @Test
  public void testSanitizePreservesInsertionOrder()
  {
    final Map<String, Object> input = new LinkedHashMap<>();
    input.put("a.first", 1);
    input.put("group.protocol", "consumer");
    input.put("b.second", 2);
    input.put("c.third", 3);

    final Map<String, Object> sanitized = ShareGroupConsumerProperties.sanitize(input);
    Assertions.assertArrayEquals(
        new String[]{"a.first", "b.second", "c.third"},
        sanitized.keySet().toArray(new String[0])
    );
  }

  @Test
  public void testSanitizeOnEmptyMap()
  {
    Assertions.assertTrue(ShareGroupConsumerProperties.sanitize(ImmutableMap.of()).isEmpty());
  }

  @Test
  public void testSanitizePropertiesRemovesUnsupportedKeysInPlace()
  {
    final Properties props = new Properties();
    props.setProperty("bootstrap.servers", "broker:9092");
    props.setProperty("enable.auto.commit", "true");
    props.setProperty("auto.offset.reset", "earliest");
    props.setProperty("group.instance.id", "test-instance");
    props.setProperty("isolation.level", "read_committed");
    props.setProperty("session.timeout.ms", "30000");

    ShareGroupConsumerProperties.sanitize(props);

    Assertions.assertEquals("broker:9092", props.getProperty("bootstrap.servers"));
    for (String key : ShareGroupConsumerProperties.UNSUPPORTED_CONFIGS) {
      Assertions.assertFalse(
          props.containsKey(key),
          "expected unsupported key removed: " + key
      );
    }
  }

  @Test
  public void testSanitizePropertiesIsNoopWhenAllKeysAllowed()
  {
    final Properties props = new Properties();
    props.setProperty("bootstrap.servers", "broker:9092");
    props.setProperty("client.id", "share-group-client");

    ShareGroupConsumerProperties.sanitize(props);

    Assertions.assertEquals(2, props.size());
    Assertions.assertEquals("broker:9092", props.getProperty("bootstrap.servers"));
    Assertions.assertEquals("share-group-client", props.getProperty("client.id"));
  }

  @Test
  public void testUnsupportedConfigsContainsKnownKafka42Keys()
  {
    Assertions.assertTrue(ShareGroupConsumerProperties.UNSUPPORTED_CONFIGS.contains("auto.offset.reset"));
    Assertions.assertTrue(ShareGroupConsumerProperties.UNSUPPORTED_CONFIGS.contains("enable.auto.commit"));
    Assertions.assertTrue(ShareGroupConsumerProperties.UNSUPPORTED_CONFIGS.contains("group.instance.id"));
    Assertions.assertTrue(ShareGroupConsumerProperties.UNSUPPORTED_CONFIGS.contains("isolation.level"));
    Assertions.assertTrue(ShareGroupConsumerProperties.UNSUPPORTED_CONFIGS.contains("partition.assignment.strategy"));
    Assertions.assertTrue(ShareGroupConsumerProperties.UNSUPPORTED_CONFIGS.contains("interceptor.classes"));
    Assertions.assertTrue(ShareGroupConsumerProperties.UNSUPPORTED_CONFIGS.contains("session.timeout.ms"));
    Assertions.assertTrue(ShareGroupConsumerProperties.UNSUPPORTED_CONFIGS.contains("heartbeat.interval.ms"));
    Assertions.assertTrue(ShareGroupConsumerProperties.UNSUPPORTED_CONFIGS.contains("group.protocol"));
    Assertions.assertTrue(ShareGroupConsumerProperties.UNSUPPORTED_CONFIGS.contains("group.remote.assignor"));
  }
}
