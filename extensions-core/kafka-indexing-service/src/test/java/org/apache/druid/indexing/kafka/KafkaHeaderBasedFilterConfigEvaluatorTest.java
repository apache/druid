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

import org.apache.druid.indexing.kafka.supervisor.KafkaHeaderBasedFilterConfig;
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

public class KafkaHeaderBasedFilterConfigEvaluatorTest
{
  private KafkaHeaderBasedFilterEvaluator evaluator;
  private ConsumerRecord<byte[], byte[]> record;

  @BeforeAll
  public static void setUpStatic()
  {
    ExpressionProcessing.initializeForTests();
  }

  @BeforeEach
  public void setUp()
  {
    // Create a test record with headers
    RecordHeaders headers = new RecordHeaders();
    headers.add(new RecordHeader("environment", "production".getBytes(StandardCharsets.UTF_8)));
    headers.add(new RecordHeader("service", "user-service".getBytes(StandardCharsets.UTF_8)));
    headers.add(new RecordHeader("version", "1.0".getBytes(StandardCharsets.UTF_8)));

    record = new ConsumerRecord<>(
        "test-topic",
        0,
        100L,
        "test-key".getBytes(StandardCharsets.UTF_8),
        "test-value".getBytes(StandardCharsets.UTF_8)
    );

    try {
      // Use reflection to set headers since ConsumerRecord doesn't have a public setter
      Field headersField = ConsumerRecord.class.getDeclaredField("headers");
      headersField.setAccessible(true);
      headersField.set(record, headers);
    }
    catch (Exception e) {
      throw new RuntimeException("Failed to set headers on test record", e);
    }
  }

  @Test
  public void testInFilterSingleValueMatch()
  {
    InDimFilter filter = new InDimFilter("environment", Collections.singletonList("production"), null);
    evaluator = new KafkaHeaderBasedFilterEvaluator(new KafkaHeaderBasedFilterConfig(filter, null, null));

    Assertions.assertTrue(evaluator.shouldIncludeRecord(record));
  }

  @Test
  public void testInFilterSingleValueNoMatch()
  {
    InDimFilter filter = new InDimFilter("environment", Collections.singletonList("staging"), null);
    evaluator = new KafkaHeaderBasedFilterEvaluator(new KafkaHeaderBasedFilterConfig(filter, null, null));

    Assertions.assertFalse(evaluator.shouldIncludeRecord(record));
  }

  @Test
  public void testInFilterMultipleValuesMatch()
  {
    InDimFilter filter = new InDimFilter("environment", Arrays.asList("staging", "production", "development"), null);
    evaluator = new KafkaHeaderBasedFilterEvaluator(new KafkaHeaderBasedFilterConfig(filter, null, null));

    Assertions.assertTrue(evaluator.shouldIncludeRecord(record));
  }

  @Test
  public void testInFilterMultipleValuesNoMatch()
  {
    InDimFilter filter = new InDimFilter("environment", Arrays.asList("staging", "development"), null);
    evaluator = new KafkaHeaderBasedFilterEvaluator(new KafkaHeaderBasedFilterConfig(filter, null, null));

    Assertions.assertFalse(evaluator.shouldIncludeRecord(record));
  }

  @Test
  public void testInFilterMissingHeader()
  {
    InDimFilter filter = new InDimFilter("missing-header", Collections.singletonList("value"), null);
    evaluator = new KafkaHeaderBasedFilterEvaluator(new KafkaHeaderBasedFilterConfig(filter, null, null));

    // With permissive filtering, missing headers should result in inclusion
    Assertions.assertTrue(evaluator.shouldIncludeRecord(record), "InDimFilter with missing header should include record (permissive)");
  }

  @Test
  public void testInFilterNullValue()
  {
    // Create record with null header value
    RecordHeaders headers = new RecordHeaders();
    headers.add(new RecordHeader("null-header", null));

    ConsumerRecord<byte[], byte[]> nullRecord = new ConsumerRecord<>(
        "test-topic",
        0,
        100L,
        "test-key".getBytes(StandardCharsets.UTF_8),
        "test-value".getBytes(StandardCharsets.UTF_8)
    );

    try {
      Field headersField = ConsumerRecord.class.getDeclaredField("headers");
      headersField.setAccessible(true);
      headersField.set(nullRecord, headers);
    }
    catch (Exception e) {
      throw new RuntimeException("Failed to set headers on test record", e);
    }

    InDimFilter filter = new InDimFilter("null-header", Collections.singletonList("value"), null);
    evaluator = new KafkaHeaderBasedFilterEvaluator(new KafkaHeaderBasedFilterConfig(filter, null, null));

    Assertions.assertTrue(evaluator.shouldIncludeRecord(nullRecord));
  }

  @Test
  public void testInFilterEmptyValueIsPermissive()
  {
    // A zero-length header value must not decode to an empty string and be matched against the filter;
    // it is treated permissively (included) like a missing or null value.
    RecordHeaders headers = new RecordHeaders();
    headers.add(new RecordHeader("environment", new byte[0]));
    ConsumerRecord<byte[], byte[]> emptyRecord = createRecordWithHeaders(headers);

    InDimFilter filter = new InDimFilter("environment", Collections.singletonList("production"), null);
    evaluator = new KafkaHeaderBasedFilterEvaluator(new KafkaHeaderBasedFilterConfig(filter, null, null));

    Assertions.assertTrue(evaluator.shouldIncludeRecord(emptyRecord), "Empty header value should include record (permissive)");
  }

  @Test
  public void testInFilterMalformedBytesArePermissive()
  {
    // 0xFF 0xFE 0xFF is not valid UTF-8. Decoding must fail (rather than silently produce a replacement string),
    // so the record is included permissively instead of being matched against a bogus decoded value.
    RecordHeaders headers = new RecordHeaders();
    headers.add(new RecordHeader("environment", new byte[]{(byte) 0xFF, (byte) 0xFE, (byte) 0xFF}));
    ConsumerRecord<byte[], byte[]> malformedRecord = createRecordWithHeaders(headers);

    InDimFilter filter = new InDimFilter("environment", Collections.singletonList("production"), null);
    evaluator = new KafkaHeaderBasedFilterEvaluator(new KafkaHeaderBasedFilterConfig(filter, "UTF-8", null));

    Assertions.assertTrue(evaluator.shouldIncludeRecord(malformedRecord), "Malformed header bytes should include record (permissive)");
  }

  private static ConsumerRecord<byte[], byte[]> createRecordWithHeaders(RecordHeaders headers)
  {
    ConsumerRecord<byte[], byte[]> newRecord = new ConsumerRecord<>(
        "test-topic",
        0,
        100L,
        "test-key".getBytes(StandardCharsets.UTF_8),
        "test-value".getBytes(StandardCharsets.UTF_8)
    );
    try {
      Field headersField = ConsumerRecord.class.getDeclaredField("headers");
      headersField.setAccessible(true);
      headersField.set(newRecord, headers);
    }
    catch (Exception e) {
      throw new RuntimeException("Failed to set headers on test record", e);
    }
    return newRecord;
  }

  @Test
  public void testInFilterWithDifferentServices()
  {
    InDimFilter filter = new InDimFilter("service", Arrays.asList("user-service", "payment-service"), null);
    evaluator = new KafkaHeaderBasedFilterEvaluator(new KafkaHeaderBasedFilterConfig(filter, null, null));

    Assertions.assertTrue(evaluator.shouldIncludeRecord(record)); // matches "user-service"
  }

  @Test
  public void testInFilterWithDifferentServicesNoMatch()
  {
    InDimFilter filter = new InDimFilter("service", Arrays.asList("payment-service", "notification-service"), null);
    evaluator = new KafkaHeaderBasedFilterEvaluator(new KafkaHeaderBasedFilterConfig(filter, null, null));

    Assertions.assertFalse(evaluator.shouldIncludeRecord(record)); // doesn't match "user-service"
  }

  @Test
  public void testRepeatedEvaluations()
  {
    InDimFilter filter = new InDimFilter("environment", Collections.singletonList("production"), null);
    evaluator = new KafkaHeaderBasedFilterEvaluator(new KafkaHeaderBasedFilterConfig(filter, null, null));

    // Test multiple evaluations to verify consistent behavior
    boolean result1 = evaluator.shouldIncludeRecord(record); // should match
    boolean result2 = evaluator.shouldIncludeRecord(record); // should match

    Assertions.assertTrue(result1, "First evaluation should match");
    Assertions.assertTrue(result2, "Second evaluation should match");
    Assertions.assertEquals(result1, result2, "Results should be consistent");
  }

  @Test
  public void testDifferentEncodings()
  {
    // Test with ISO-8859-1 encoding
    String testValue = "café"; // Contains non-ASCII characters

    RecordHeaders headers = new RecordHeaders();
    headers.add(new RecordHeader("text", testValue.getBytes(StandardCharsets.ISO_8859_1)));

    ConsumerRecord<byte[], byte[]> encodedRecord = new ConsumerRecord<>(
        "test-topic",
        0,
        100L,
        "test-key".getBytes(StandardCharsets.UTF_8),
        "test-value".getBytes(StandardCharsets.UTF_8)
    );

    try {
      Field headersField = ConsumerRecord.class.getDeclaredField("headers");
      headersField.setAccessible(true);
      headersField.set(encodedRecord, headers);
    }
    catch (Exception e) {
      throw new RuntimeException("Failed to set headers on test record", e);
    }

    InDimFilter filter = new InDimFilter("text", Collections.singletonList(testValue), null);
    evaluator = new KafkaHeaderBasedFilterEvaluator(new KafkaHeaderBasedFilterConfig(filter, "ISO-8859-1", null));

    Assertions.assertTrue(evaluator.shouldIncludeRecord(encodedRecord));
  }

  @Test
  public void testNullHeaderValue()
  {
    // Create record without the header we're filtering on
    RecordHeaders headers = new RecordHeaders();
    headers.add(new RecordHeader("other-header", "other-value".getBytes(StandardCharsets.UTF_8)));

    ConsumerRecord<byte[], byte[]> noHeaderRecord = new ConsumerRecord<>(
        "test-topic",
        0,
        100L,
        "test-key".getBytes(StandardCharsets.UTF_8),
        "test-value".getBytes(StandardCharsets.UTF_8)
    );

    try {
      Field headersField = ConsumerRecord.class.getDeclaredField("headers");
      headersField.setAccessible(true);
      headersField.set(noHeaderRecord, headers);
    }
    catch (Exception e) {
      throw new RuntimeException("Failed to set headers on test record", e);
    }

    InDimFilter filter = new InDimFilter("environment", Collections.singletonList("production"), null);
    evaluator = new KafkaHeaderBasedFilterEvaluator(new KafkaHeaderBasedFilterConfig(filter, null, null));

    // Missing header should result in inclusion (permissive behavior)
    Assertions.assertTrue(evaluator.shouldIncludeRecord(noHeaderRecord));
  }

  @Test
  public void testMultipleHeadersWithSameKey()
  {
    // Create record with multiple headers with the same key (Kafka allows this)
    RecordHeaders headers = new RecordHeaders();
    headers.add(new RecordHeader("environment", "staging".getBytes(StandardCharsets.UTF_8)));
    headers.add(new RecordHeader("environment", "production".getBytes(StandardCharsets.UTF_8))); // Last one wins

    ConsumerRecord<byte[], byte[]> multiHeaderRecord = new ConsumerRecord<>(
        "test-topic",
        0,
        100L,
        "test-key".getBytes(StandardCharsets.UTF_8),
        "test-value".getBytes(StandardCharsets.UTF_8)
    );

    try {
      Field headersField = ConsumerRecord.class.getDeclaredField("headers");
      headersField.setAccessible(true);
      headersField.set(multiHeaderRecord, headers);
    }
    catch (Exception e) {
      throw new RuntimeException("Failed to set headers on test record", e);
    }

    // Filter should match "production" (the last value), not "staging" (the first value)
    InDimFilter prodFilter = new InDimFilter("environment", Collections.singletonList("production"), null);
    evaluator = new KafkaHeaderBasedFilterEvaluator(new KafkaHeaderBasedFilterConfig(prodFilter, null, null));
    Assertions.assertTrue(evaluator.shouldIncludeRecord(multiHeaderRecord), "Should match last header value 'production'");

    // Filter should NOT match "staging" (the first value)
    InDimFilter stagingFilter = new InDimFilter("environment", Collections.singletonList("staging"), null);
    evaluator = new KafkaHeaderBasedFilterEvaluator(new KafkaHeaderBasedFilterConfig(stagingFilter, null, null));
    Assertions.assertFalse(evaluator.shouldIncludeRecord(multiHeaderRecord), "Should not match first header value 'staging'");
  }

  @Test
  public void testStringDecodingCacheSize()
  {
    InDimFilter filter = new InDimFilter("environment", Collections.singletonList("production"), null);
    evaluator = new KafkaHeaderBasedFilterEvaluator(new KafkaHeaderBasedFilterConfig(filter, null, 50_000));

    // Test that the evaluator works with custom cache size
    Assertions.assertTrue(evaluator.shouldIncludeRecord(record));
  }
}
