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
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Unit tests for {@link ShareGroupConsumerProperties}. Validates that the
 * sanitize method strips exactly the share-group-unsupported keys without
 * touching legitimate ones, and preserves insertion order.
 */
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
    Assert.assertEquals(input, sanitized);
  }

  @Test
  public void testSanitizeStripsAutoOffsetReset()
  {
    final Map<String, Object> input = ImmutableMap.of(
        "bootstrap.servers", "broker:9092",
        "auto.offset.reset", "earliest"
    );
    final Map<String, Object> sanitized = ShareGroupConsumerProperties.sanitize(input);
    Assert.assertEquals(1, sanitized.size());
    Assert.assertEquals("broker:9092", sanitized.get("bootstrap.servers"));
    Assert.assertFalse(sanitized.containsKey("auto.offset.reset"));
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
    Assert.assertEquals(1, sanitized.size());
    Assert.assertEquals("broker:9092", sanitized.get("bootstrap.servers"));
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
    Assert.assertArrayEquals(
        new String[]{"a.first", "b.second", "c.third"},
        sanitized.keySet().toArray(new String[0])
    );
  }

  @Test
  public void testSanitizeOnEmptyMap()
  {
    Assert.assertTrue(ShareGroupConsumerProperties.sanitize(ImmutableMap.of()).isEmpty());
  }

  @Test
  public void testUnsupportedConfigsContainsKnownKafka42Keys()
  {
    Assert.assertTrue(ShareGroupConsumerProperties.UNSUPPORTED_CONFIGS.contains("auto.offset.reset"));
    Assert.assertTrue(ShareGroupConsumerProperties.UNSUPPORTED_CONFIGS.contains("enable.auto.commit"));
    Assert.assertTrue(ShareGroupConsumerProperties.UNSUPPORTED_CONFIGS.contains("group.instance.id"));
    Assert.assertTrue(ShareGroupConsumerProperties.UNSUPPORTED_CONFIGS.contains("isolation.level"));
    Assert.assertTrue(ShareGroupConsumerProperties.UNSUPPORTED_CONFIGS.contains("partition.assignment.strategy"));
    Assert.assertTrue(ShareGroupConsumerProperties.UNSUPPORTED_CONFIGS.contains("interceptor.classes"));
    Assert.assertTrue(ShareGroupConsumerProperties.UNSUPPORTED_CONFIGS.contains("session.timeout.ms"));
    Assert.assertTrue(ShareGroupConsumerProperties.UNSUPPORTED_CONFIGS.contains("heartbeat.interval.ms"));
    Assert.assertTrue(ShareGroupConsumerProperties.UNSUPPORTED_CONFIGS.contains("group.protocol"));
    Assert.assertTrue(ShareGroupConsumerProperties.UNSUPPORTED_CONFIGS.contains("group.remote.assignor"));
  }
}
