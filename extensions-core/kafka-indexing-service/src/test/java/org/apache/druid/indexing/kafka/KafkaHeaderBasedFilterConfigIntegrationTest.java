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
import org.apache.druid.indexing.kafka.supervisor.KafkaHeaderBasedFilterConfig;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

public class KafkaHeaderBasedFilterConfigIntegrationTest
{
  private KafkaHeaderBasedFilterEvaluator evaluator;
  private final ObjectMapper objectMapper = new DefaultObjectMapper();

  @BeforeAll
  public static void setUpStatic()
  {
    ExpressionProcessing.initializeForTests();
  }

  @BeforeEach
  public void setUp()
  {
    // Will be initialized in each test
    evaluator = null;
  }

  private ConsumerRecord<byte[], byte[]> createRecord(String topic, int partition, long offset, RecordHeaders headers)
  {
    ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
        topic,
        partition,
        offset,
        "test-key".getBytes(StandardCharsets.UTF_8),
        "test-value".getBytes(StandardCharsets.UTF_8)
    );

    try {
      Field headersField = ConsumerRecord.class.getDeclaredField("headers");
      headersField.setAccessible(true);
      headersField.set(record, headers);
    }
    catch (Exception e) {
      throw new RuntimeException("Failed to set headers on test record", e);
    }

    return record;
  }

  private RecordHeaders headers(String... keyValuePairs)
  {
    RecordHeaders headers = new RecordHeaders();
    for (int i = 0; i < keyValuePairs.length - 1; i += 2) {
      headers.add(new RecordHeader(keyValuePairs[i], keyValuePairs[i + 1].getBytes(StandardCharsets.UTF_8)));
    }
    return headers;
  }

  @Test
  public void testProductionEnvironmentFiltering()
  {
    // Test Case: Only include records from production environment
    InDimFilter filter = new InDimFilter("environment", Collections.singletonList("production"), null);
    KafkaHeaderBasedFilterConfig headerFilter = new KafkaHeaderBasedFilterConfig(filter, null, null);
    evaluator = new KafkaHeaderBasedFilterEvaluator(headerFilter);

    // Production record - should be included
    ConsumerRecord<byte[], byte[]> prodRecord = createRecord(
        "events",
        0,
        100L,
        headers("environment", "production", "service", "user-service")
    );
    Assertions.assertTrue(evaluator.shouldIncludeRecord(prodRecord), "Production record should be included");

    // Staging record - should be excluded
    ConsumerRecord<byte[], byte[]> stagingRecord = createRecord(
        "events",
        0,
        101L,
        headers("environment", "staging", "service", "user-service")
    );
    Assertions.assertFalse(evaluator.shouldIncludeRecord(stagingRecord), "Staging record should be excluded");

    // Record without environment header - should be included (permissive)
    ConsumerRecord<byte[], byte[]> noEnvRecord = createRecord(
        "events",
        0,
        102L,
        headers("service", "user-service")
    );
    Assertions.assertTrue(evaluator.shouldIncludeRecord(noEnvRecord), "Record without environment header should be included");
  }

  @Test
  public void testMultiServiceFiltering()
  {
    // Test Case: Include records from multiple services
    InDimFilter filter = new InDimFilter("service", Arrays.asList("user-service", "payment-service"), null);
    KafkaHeaderBasedFilterConfig headerFilter = new KafkaHeaderBasedFilterConfig(filter, null, null);
    evaluator = new KafkaHeaderBasedFilterEvaluator(headerFilter);

    // User service record - should be included
    ConsumerRecord<byte[], byte[]> userRecord = createRecord(
        "events",
        0,
        100L,
        headers("environment", "production", "service", "user-service")
    );
    Assertions.assertTrue(evaluator.shouldIncludeRecord(userRecord), "User service record should be included");

    // Payment service record - should be included
    ConsumerRecord<byte[], byte[]> paymentRecord = createRecord(
        "events",
        0,
        101L,
        headers("environment", "production", "service", "payment-service")
    );
    Assertions.assertTrue(evaluator.shouldIncludeRecord(paymentRecord), "Payment service record should be included");

    // Notification service record - should be excluded
    ConsumerRecord<byte[], byte[]> notificationRecord = createRecord(
        "events",
        0,
        102L,
        headers("environment", "production", "service", "notification-service")
    );
    Assertions.assertFalse(evaluator.shouldIncludeRecord(notificationRecord), "Notification service record should be excluded");
  }

  @Test
  public void testHighThroughputFiltering()
  {
    // Test Case: Filter for high-throughput scenarios with many values
    InDimFilter filter = new InDimFilter(
        "metric.name",
        Arrays.asList(
            "io.kafka.server/delayed_share_fetch/expires/total/delta",
            "io.kafka.server/request_bytes/total/delta",
            "io.kafka.server/response_bytes/total/delta"
        ),
        null
    );
    KafkaHeaderBasedFilterConfig headerFilter = new KafkaHeaderBasedFilterConfig(filter, null, null);
    evaluator = new KafkaHeaderBasedFilterEvaluator(headerFilter);

    // Matching metric - should be included
    ConsumerRecord<byte[], byte[]> matchingRecord = createRecord(
        "telemetry.metrics.cloud.stag",
        0,
        100L,
        headers("metric.name", "io.kafka.server/delayed_share_fetch/expires/total/delta")
    );
    Assertions.assertTrue(evaluator.shouldIncludeRecord(matchingRecord), "Matching metric should be included");

    // Non-matching metric - should be excluded
    ConsumerRecord<byte[], byte[]> nonMatchingRecord = createRecord(
        "telemetry.metrics.cloud.stag",
        0,
        101L,
        headers("metric.name", "some.other.metric")
    );
    Assertions.assertFalse(evaluator.shouldIncludeRecord(nonMatchingRecord), "Non-matching metric should be excluded");
  }

  @Test
  public void testFilteringBehavior()
  {
    // Test Case: Verify basic filtering behavior
    InDimFilter filter = new InDimFilter("environment", Collections.singletonList("production"), null);
    KafkaHeaderBasedFilterConfig headerFilter = new KafkaHeaderBasedFilterConfig(filter, null, null);
    evaluator = new KafkaHeaderBasedFilterEvaluator(headerFilter);

    // Process multiple records to verify filtering logic
    ConsumerRecord<byte[], byte[]> record1 = createRecord("events", 0, 100L, headers("environment", "production"));
    ConsumerRecord<byte[], byte[]> record2 = createRecord("events", 0, 101L, headers("environment", "staging"));
    ConsumerRecord<byte[], byte[]> record3 = createRecord("events", 0, 102L, headers("environment", "production"));

    // Verify filtering results
    Assertions.assertTrue(evaluator.shouldIncludeRecord(record1), "Production record should be included");
    Assertions.assertFalse(evaluator.shouldIncludeRecord(record2), "Staging record should be excluded");
    Assertions.assertTrue(evaluator.shouldIncludeRecord(record3), "Production record should be included");
  }

  @Test
  public void testConfigurationSerialization() throws Exception
  {
    // Test that header filter configurations can be serialized/deserialized correctly
    InDimFilter filter = new InDimFilter("environment", Arrays.asList("production", "staging"), null);
    KafkaHeaderBasedFilterConfig originalFilter = new KafkaHeaderBasedFilterConfig(filter, "UTF-16", null);

    // Serialize to JSON
    String json = objectMapper.writeValueAsString(originalFilter);

    // Deserialize back
    KafkaHeaderBasedFilterConfig deserializedFilter = objectMapper.readValue(json, KafkaHeaderBasedFilterConfig.class);

    // Verify they're equivalent
    Assertions.assertEquals(originalFilter.getFilter(), deserializedFilter.getFilter());
    Assertions.assertEquals(originalFilter.getEncoding(), deserializedFilter.getEncoding());
    Assertions.assertEquals(originalFilter.getStringDecodingCacheSize(), deserializedFilter.getStringDecodingCacheSize());

    // Test that the deserialized filter works
    evaluator = new KafkaHeaderBasedFilterEvaluator(deserializedFilter);
    ConsumerRecord<byte[], byte[]> record = createRecord("events", 0, 100L, headers("environment", "production"));
    Assertions.assertFalse(evaluator.shouldIncludeRecord(record), "Deserialized filter should work");
  }

  @Test
  public void testEncodingHandling()
  {
    // Test different character encodings
    String testValue = "café"; // Contains non-ASCII characters

    InDimFilter filter = new InDimFilter("text", Collections.singletonList(testValue), null);
    KafkaHeaderBasedFilterConfig headerFilter = new KafkaHeaderBasedFilterConfig(filter, "ISO-8859-1", null);
    evaluator = new KafkaHeaderBasedFilterEvaluator(headerFilter);

    // Create record with ISO-8859-1 encoded header
    RecordHeaders headers = new RecordHeaders();
    headers.add(new RecordHeader("text", testValue.getBytes(StandardCharsets.ISO_8859_1)));

    ConsumerRecord<byte[], byte[]> record = createRecord("events", 0, 100L, headers);

    Assertions.assertTrue(evaluator.shouldIncludeRecord(record), "Should handle different encodings correctly");
  }

  @Test
  public void testCustomCacheSize()
  {
    // Test with custom cache size
    InDimFilter filter = new InDimFilter("environment", Collections.singletonList("production"), null);
    KafkaHeaderBasedFilterConfig headerFilter = new KafkaHeaderBasedFilterConfig(filter, null, 100_000);
    evaluator = new KafkaHeaderBasedFilterEvaluator(headerFilter);

    ConsumerRecord<byte[], byte[]> record = createRecord("events", 0, 100L, headers("environment", "production"));
    Assertions.assertTrue(evaluator.shouldIncludeRecord(record), "Should work with custom cache size");
  }
}
