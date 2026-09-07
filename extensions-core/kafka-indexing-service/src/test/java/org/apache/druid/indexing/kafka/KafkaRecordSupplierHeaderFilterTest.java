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

import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.data.input.kafka.KafkaTopicPartition;
import org.apache.druid.indexing.kafka.supervisor.KafkaHeaderBasedFilterConfig;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test KafkaRecordSupplier with header-based filtering integrated into the main poll() method.
 */
public class KafkaRecordSupplierHeaderFilterTest
{
  private KafkaConsumer<byte[], byte[]> mockConsumer;

  private KafkaRecordSupplier recordSupplier;

  @BeforeAll
  public static void setUpClass()
  {
    ExpressionProcessing.initializeForTests();
  }

  @BeforeEach
  public void setUp()
  {
    mockConsumer = EasyMock.createMock(KafkaConsumer.class);
  }

  @Test
  public void testNoHeaderFilter()
  {
    // Test that records are not filtered when no header filter is configured
    recordSupplier = new KafkaRecordSupplier(mockConsumer, false, null);

    ConsumerRecord<byte[], byte[]> record1 = createRecord("topic", 0, 100L,
        headers("environment", "production"));
    ConsumerRecord<byte[], byte[]> record2 = createRecord("topic", 0, 101L,
        headers("environment", "staging"));

    EasyMock.expect(mockConsumer.poll(EasyMock.anyObject(Duration.class)))
        .andReturn(createConsumerRecords(Arrays.asList(record1, record2)));
    EasyMock.replay(mockConsumer);

    List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> results =
        recordSupplier.poll(1000);

    Assertions.assertEquals(2, results.size(), "Should include all records when no filter");
    Assertions.assertEquals(100L, (long) results.get(0).getSequenceNumber());
    Assertions.assertEquals(101L, (long) results.get(1).getSequenceNumber());
    EasyMock.verify(mockConsumer);
  }

  @Test
  public void testInHeaderFilterSingleValue()
  {
    // Test filtering with in filter (single value)
    InDimFilter filter = new InDimFilter("environment", Collections.singletonList("production"), null);
    KafkaHeaderBasedFilterConfig headerFilter = new KafkaHeaderBasedFilterConfig(filter, null, null);

    recordSupplier = new KafkaRecordSupplier(mockConsumer, false, null, headerFilter);

    ConsumerRecord<byte[], byte[]> prodRecord = createRecord("topic", 0, 100L,
        headers("environment", "production"));
    ConsumerRecord<byte[], byte[]> stagingRecord = createRecord("topic", 0, 101L,
        headers("environment", "staging"));
    ConsumerRecord<byte[], byte[]> noHeaderRecord = createRecord("topic", 0, 102L,
        new RecordHeaders()); // No headers

    EasyMock.expect(mockConsumer.poll(EasyMock.anyObject(Duration.class)))
        .andReturn(createConsumerRecords(Arrays.asList(prodRecord, stagingRecord, noHeaderRecord)));
    EasyMock.replay(mockConsumer);

    List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> results =
        recordSupplier.poll(1000);

    Assertions.assertEquals(3, results.size(), "Should return all records (accepted + filtered markers)");

    // First record: production (accepted - has data)
    Assertions.assertNotNull(results.get(0).getData(), "Production record should have data");
    Assertions.assertFalse(results.get(0).getData().isEmpty(), "Production record should have data");
    Assertions.assertEquals(100L, (long) results.get(0).getSequenceNumber());

    // Second record: staging (filtered - marked filtered but payload retained so input bytes are still counted)
    Assertions.assertTrue(results.get(1).isFiltered(), "Staging record should be filtered");
    Assertions.assertFalse(results.get(1).getData().isEmpty(), "Filtered record should retain payload data");
    Assertions.assertEquals(101L, (long) results.get(1).getSequenceNumber());

    // Third record: no-header (accepted - has data, permissive behavior)
    Assertions.assertNotNull(results.get(2).getData(), "No-header record should have data");
    Assertions.assertFalse(results.get(2).getData().isEmpty(), "No-header record should have data");
    Assertions.assertEquals(102L, (long) results.get(2).getSequenceNumber());
    EasyMock.verify(mockConsumer);
  }

  @Test
  public void testFilteredFlagTracking()
  {
    // Test that filtered records are properly marked with filtered flag
    InDimFilter filter = new InDimFilter("environment", Collections.singletonList("production"), null);
    KafkaHeaderBasedFilterConfig headerFilter = new KafkaHeaderBasedFilterConfig(filter, null, null);

    recordSupplier = new KafkaRecordSupplier(mockConsumer, false, null, headerFilter);

    ConsumerRecord<byte[], byte[]> prodRecord = createRecord("topic", 0, 100L,
        headers("environment", "production"));
    ConsumerRecord<byte[], byte[]> stagingRecord = createRecord("topic", 0, 101L,
        headers("environment", "staging"));

    EasyMock.expect(mockConsumer.poll(EasyMock.anyObject(Duration.class)))
        .andReturn(createConsumerRecords(Arrays.asList(prodRecord, stagingRecord)));
    EasyMock.replay(mockConsumer);

    List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> results =
        recordSupplier.poll(1000);

    // Verify records returned
    Assertions.assertEquals(2, results.size(), "Should return 2 records (accepted + filtered)");

    // Verify filtered flags
    Assertions.assertFalse(results.get(0).isFiltered(), "Production record should not be filtered");
    Assertions.assertTrue(results.get(1).isFiltered(), "Staging record should be filtered");

    // Verify data presence: filtered records retain their payload so input bytes are still accounted for.
    Assertions.assertFalse(results.get(0).getData().isEmpty(), "Production record should have data");
    Assertions.assertFalse(results.get(1).getData().isEmpty(), "Filtered record should retain payload data");

    EasyMock.verify(mockConsumer);
  }

  @Test
  public void testInHeaderFilterMultipleValues()
  {
    // Test filtering with in filter (multiple values)
    InDimFilter filter = new InDimFilter("service", Arrays.asList("user-service", "payment-service"), null);
    KafkaHeaderBasedFilterConfig headerFilter = new KafkaHeaderBasedFilterConfig(filter, null, null);

    recordSupplier = new KafkaRecordSupplier(mockConsumer, false, null, headerFilter);

    ConsumerRecord<byte[], byte[]> userServiceRecord = createRecord("topic", 0, 100L,
        headers("service", "user-service"));
    ConsumerRecord<byte[], byte[]> paymentServiceRecord = createRecord("topic", 0, 101L,
        headers("service", "payment-service"));
    ConsumerRecord<byte[], byte[]> orderServiceRecord = createRecord("topic", 0, 102L,
        headers("service", "order-service"));

    EasyMock.expect(mockConsumer.poll(EasyMock.anyObject(Duration.class)))
        .andReturn(createConsumerRecords(Arrays.asList(userServiceRecord, paymentServiceRecord, orderServiceRecord)));
    EasyMock.replay(mockConsumer);

    List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> results =
        recordSupplier.poll(1000);

    Assertions.assertEquals(3, results.size(), "Should return all records (accepted + filtered markers)");

    // First record: user-service (accepted - has data)
    Assertions.assertNotNull(results.get(0).getData(), "User-service record should have data");
    Assertions.assertFalse(results.get(0).getData().isEmpty(), "User-service record should have data");
    Assertions.assertEquals(100L, (long) results.get(0).getSequenceNumber());

    // Second record: payment-service (accepted - has data)
    Assertions.assertNotNull(results.get(1).getData(), "Payment-service record should have data");
    Assertions.assertFalse(results.get(1).getData().isEmpty(), "Payment-service record should have data");
    Assertions.assertEquals(101L, (long) results.get(1).getSequenceNumber());

    // Third record: order-service (filtered - marked filtered but payload retained for byte accounting)
    Assertions.assertTrue(results.get(2).isFiltered(), "Order-service record should be filtered");
    Assertions.assertFalse(results.get(2).getData().isEmpty(), "Filtered record should retain payload data");
    Assertions.assertEquals(102L, (long) results.get(2).getSequenceNumber());
    EasyMock.verify(mockConsumer);
  }

  @Test
  public void testInFilterWithMultipleHeaders()
  {
    // Test InDimFilter with multiple possible values
    InDimFilter serviceFilter = new InDimFilter("service", Arrays.asList("user-service", "payment-service"), null);
    KafkaHeaderBasedFilterConfig headerFilter = new KafkaHeaderBasedFilterConfig(serviceFilter, null, null);

    recordSupplier = new KafkaRecordSupplier(mockConsumer, false, null, headerFilter);

    ConsumerRecord<byte[], byte[]> userServiceRecord = createRecord("topic", 0, 100L,
        headers("service", "user-service"));
    ConsumerRecord<byte[], byte[]> paymentServiceRecord = createRecord("topic", 0, 101L,
        headers("service", "payment-service"));
    ConsumerRecord<byte[], byte[]> orderServiceRecord = createRecord("topic", 0, 102L,
        headers("service", "order-service"));

    EasyMock.expect(mockConsumer.poll(EasyMock.anyObject(Duration.class)))
        .andReturn(createConsumerRecords(Arrays.asList(userServiceRecord, paymentServiceRecord, orderServiceRecord)));
    EasyMock.replay(mockConsumer);

    List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> results =
        recordSupplier.poll(1000);

    Assertions.assertEquals(3, results.size(), "Should return all records (accepted + filtered markers)");

    // First record: user-service (accepted - has data)
    Assertions.assertNotNull(results.get(0).getData(), "User-service record should have data");
    Assertions.assertFalse(results.get(0).getData().isEmpty(), "User-service record should have data");
    Assertions.assertEquals(100L, (long) results.get(0).getSequenceNumber());

    // Second record: payment-service (accepted - has data)
    Assertions.assertNotNull(results.get(1).getData(), "Payment-service record should have data");
    Assertions.assertFalse(results.get(1).getData().isEmpty(), "Payment-service record should have data");
    Assertions.assertEquals(101L, (long) results.get(1).getSequenceNumber());

    // Third record: order-service (filtered - marked filtered but payload retained for byte accounting)
    Assertions.assertTrue(results.get(2).isFiltered(), "Order-service record should be filtered");
    Assertions.assertFalse(results.get(2).getData().isEmpty(), "Filtered record should retain payload data");
    Assertions.assertEquals(102L, (long) results.get(2).getSequenceNumber());
    EasyMock.verify(mockConsumer);
  }

  @Test
  public void testMultiplePolls()
  {
    // Test that statistics accumulate across multiple polls
    InDimFilter filter = new InDimFilter("environment", Collections.singletonList("production"), null);
    KafkaHeaderBasedFilterConfig headerFilter = new KafkaHeaderBasedFilterConfig(filter, null, null);

    recordSupplier = new KafkaRecordSupplier(mockConsumer, false, null, headerFilter);

    // First poll
    ConsumerRecord<byte[], byte[]> prodRecord1 = createRecord("topic", 0, 100L,
        headers("environment", "production"));
    ConsumerRecord<byte[], byte[]> stagingRecord1 = createRecord("topic", 0, 101L,
        headers("environment", "staging"));

    // Second poll
    ConsumerRecord<byte[], byte[]> prodRecord2 = createRecord("topic", 0, 102L,
        headers("environment", "production"));
    ConsumerRecord<byte[], byte[]> stagingRecord2 = createRecord("topic", 0, 103L,
        headers("environment", "staging"));

    EasyMock.expect(mockConsumer.poll(EasyMock.anyObject(Duration.class)))
        .andReturn(createConsumerRecords(Arrays.asList(prodRecord1, stagingRecord1)));
    EasyMock.expect(mockConsumer.poll(EasyMock.anyObject(Duration.class)))
        .andReturn(createConsumerRecords(Arrays.asList(prodRecord2, stagingRecord2)));
    EasyMock.replay(mockConsumer);

    List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> results1 =
        recordSupplier.poll(1000);

    Assertions.assertEquals(2, results1.size(), "First poll should return 2 records (accepted + filtered marker)");
    Assertions.assertNotNull(results1.get(0).getData(), "Production record should have data");
    Assertions.assertFalse(results1.get(0).getData().isEmpty(), "Production record should have data");
    Assertions.assertFalse(results1.get(1).getData().isEmpty(), "Filtered record should retain payload data");

    List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> results2 =
        recordSupplier.poll(1000);

    Assertions.assertEquals(2, results2.size(), "Second poll should return 2 records (accepted + filtered marker)");
    Assertions.assertNotNull(results2.get(0).getData(), "Production record should have data");
    Assertions.assertFalse(results2.get(0).getData().isEmpty(), "Production record should have data");
    Assertions.assertFalse(results2.get(1).getData().isEmpty(), "Filtered record should retain payload data");
    EasyMock.verify(mockConsumer);
  }

  @Test
  public void testEmptyPoll()
  {
    // Test that empty polls don't affect statistics
    InDimFilter filter = new InDimFilter("environment", Collections.singletonList("production"), null);
    KafkaHeaderBasedFilterConfig headerFilter = new KafkaHeaderBasedFilterConfig(filter, null, null);

    recordSupplier = new KafkaRecordSupplier(mockConsumer, false, null, headerFilter);

    EasyMock.expect(mockConsumer.poll(EasyMock.anyObject(Duration.class)))
        .andReturn(createConsumerRecords(Collections.emptyList()));
    EasyMock.replay(mockConsumer);

    List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> results =
        recordSupplier.poll(1000);

    Assertions.assertEquals(0, results.size(), "Empty poll should return empty list");
    EasyMock.verify(mockConsumer);
  }

  @Test
  public void testAllRecordsFilteredStillAdvanceOffsets()
  {
    // CRITICAL TEST: Verify that when ALL records are filtered out, we still return
    // filtered record markers to prevent infinite loop
    InDimFilter filter = new InDimFilter("environment", Collections.singletonList("production"), null);
    KafkaHeaderBasedFilterConfig headerFilter = new KafkaHeaderBasedFilterConfig(filter, null, null);

    recordSupplier = new KafkaRecordSupplier(mockConsumer, false, null, headerFilter);

    // All records have "staging" environment - none should pass the "production" filter
    ConsumerRecord<byte[], byte[]> stagingRecord1 = createRecord("topic", 0, 100L,
        headers("environment", "staging"));
    ConsumerRecord<byte[], byte[]> stagingRecord2 = createRecord("topic", 0, 101L,
        headers("environment", "staging"));
    ConsumerRecord<byte[], byte[]> stagingRecord3 = createRecord("topic", 0, 102L,
        headers("environment", "staging"));

    EasyMock.expect(mockConsumer.poll(EasyMock.anyObject(Duration.class)))
        .andReturn(createConsumerRecords(Arrays.asList(stagingRecord1, stagingRecord2, stagingRecord3)));
    EasyMock.replay(mockConsumer);

    List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> results =
        recordSupplier.poll(1000);

    // CRITICAL: Even though all records were filtered, we should still get record markers
    // to advance offsets and prevent infinite loop
    Assertions.assertEquals(3, results.size(), "Should return filtered record markers for offset advancement");

    // Verify that all returned records are marked as filtered markers (used to advance offsets).
    for (OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity> result : results) {
      Assertions.assertTrue(result.isFiltered(), "Filtered record should be marked filtered");
    }

    // Verify offsets are correct
    Assertions.assertEquals(100L, (long) results.get(0).getSequenceNumber());
    Assertions.assertEquals(101L, (long) results.get(1).getSequenceNumber());
    Assertions.assertEquals(102L, (long) results.get(2).getSequenceNumber());

    EasyMock.verify(mockConsumer);
  }

  @Test
  public void testMixedFilteredAndAcceptedRecords()
  {
    // Test that mix of filtered and accepted records works correctly
    InDimFilter filter = new InDimFilter("environment", Collections.singletonList("production"), null);
    KafkaHeaderBasedFilterConfig headerFilter = new KafkaHeaderBasedFilterConfig(filter, null, null);

    recordSupplier = new KafkaRecordSupplier(mockConsumer, false, null, headerFilter);

    ConsumerRecord<byte[], byte[]> prodRecord = createRecord("topic", 0, 100L,
        headers("environment", "production"));
    ConsumerRecord<byte[], byte[]> stagingRecord = createRecord("topic", 0, 101L,
        headers("environment", "staging"));
    ConsumerRecord<byte[], byte[]> prodRecord2 = createRecord("topic", 0, 102L,
        headers("environment", "production"));

    EasyMock.expect(mockConsumer.poll(EasyMock.anyObject(Duration.class)))
        .andReturn(createConsumerRecords(Arrays.asList(prodRecord, stagingRecord, prodRecord2)));
    EasyMock.replay(mockConsumer);

    List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> results =
        recordSupplier.poll(1000);

    Assertions.assertEquals(3, results.size(), "Should return all records (accepted + filtered markers)");

    // First record: accepted (has data)
    Assertions.assertNotNull(results.get(0).getData(), "Accepted record should have data");
    Assertions.assertEquals(100L, (long) results.get(0).getSequenceNumber());

    // Second record: filtered (marked filtered, payload retained for byte accounting)
    Assertions.assertFalse(results.get(1).getData().isEmpty(), "Filtered record should retain payload data");
    Assertions.assertEquals(101L, (long) results.get(1).getSequenceNumber());

    // Third record: accepted (has data)
    Assertions.assertNotNull(results.get(2).getData(), "Accepted record should have data");
    Assertions.assertEquals(102L, (long) results.get(2).getSequenceNumber());

    EasyMock.verify(mockConsumer);
  }

  @Test
  public void testMultiTopic()
  {
    // Test header filtering with multi-topic configuration
    InDimFilter filter = new InDimFilter("environment", Collections.singletonList("production"), null);
    KafkaHeaderBasedFilterConfig headerFilter = new KafkaHeaderBasedFilterConfig(filter, null, null);

    recordSupplier = new KafkaRecordSupplier(mockConsumer, true, null, headerFilter); // multiTopic = true

    ConsumerRecord<byte[], byte[]> topic1Record = createRecord("topic1", 0, 100L,
        headers("environment", "production"));
    ConsumerRecord<byte[], byte[]> topic2Record = createRecord("topic2", 0, 101L,
        headers("environment", "staging"));

    EasyMock.expect(mockConsumer.poll(EasyMock.anyObject(Duration.class)))
        .andReturn(createConsumerRecords(Arrays.asList(topic1Record, topic2Record)));
    EasyMock.replay(mockConsumer);

    List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> results =
        recordSupplier.poll(1000);

    Assertions.assertEquals(2, results.size(), "Should return both records (accepted + filtered marker)");

    // First record: accepted
    Assertions.assertNotNull(results.get(0).getData(), "Production record should have data");
    Assertions.assertEquals("topic1", results.get(0).getStream());
    Assertions.assertTrue(
        results.get(0).getPartitionId().isMultiTopicPartition(), "Should be multi-topic partition");

    // Second record: filtered marker
    Assertions.assertFalse(results.get(1).getData().isEmpty(), "Filtered record should retain payload data");
    Assertions.assertEquals("topic2", results.get(1).getStream());

    EasyMock.verify(mockConsumer);
  }

  // Helper methods

  private ConsumerRecord<byte[], byte[]> createRecord(String topic, int partition, long offset, RecordHeaders headers)
  {
    ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
        topic,
        partition,
        offset,
        "test-key".getBytes(StandardCharsets.UTF_8),
        "test-value".getBytes(StandardCharsets.UTF_8)
    );

    // Set headers using reflection since ConsumerRecord headers are final
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
    if (keyValuePairs.length % 2 != 0) {
      throw new IllegalArgumentException("Key-value pairs must be even number of arguments");
    }

    RecordHeaders headers = new RecordHeaders();
    for (int i = 0; i < keyValuePairs.length; i += 2) {
      String key = keyValuePairs[i];
      String value = keyValuePairs[i + 1];
      headers.add(new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8)));
    }
    return headers;
  }

  private ConsumerRecords<byte[], byte[]> createConsumerRecords(List<ConsumerRecord<byte[], byte[]>> records)
  {
    Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordsMap = new HashMap<>();
    for (ConsumerRecord<byte[], byte[]> record : records) {
      TopicPartition tp = new TopicPartition(record.topic(), record.partition());
      recordsMap.computeIfAbsent(tp, k -> new ArrayList<>()).add(record);
    }
    return new ConsumerRecords<>(recordsMap);
  }
}
