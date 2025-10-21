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
import org.apache.druid.indexing.kafka.supervisor.KafkaHeaderBasedFilteringConfig;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

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

  @BeforeClass
  public static void setUpClass()
  {
    ExpressionProcessing.initializeForTests();
  }

  @Before
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

    Assert.assertEquals("Should include all records when no filter", 2, results.size());
    Assert.assertEquals(100L, (long) results.get(0).getSequenceNumber());
    Assert.assertEquals(101L, (long) results.get(1).getSequenceNumber());
    EasyMock.verify(mockConsumer);
  }

  @Test
  public void testInHeaderFilterSingleValue()
  {
    // Test filtering with in filter (single value)
    InDimFilter filter = new InDimFilter("environment", Collections.singletonList("production"), null);
    KafkaHeaderBasedFilteringConfig headerFilter = new KafkaHeaderBasedFilteringConfig(filter, null, null);

    recordSupplier = new KafkaRecordSupplier(mockConsumer, false, headerFilter);

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

    Assert.assertEquals("Should return all records (accepted + filtered markers)", 3, results.size());

    // First record: production (accepted - has data)
    Assert.assertNotNull("Production record should have data", results.get(0).getData());
    Assert.assertFalse("Production record should have data", results.get(0).getData().isEmpty());
    Assert.assertEquals(100L, (long) results.get(0).getSequenceNumber());

    // Second record: staging (filtered - empty data for offset advancement)
    Assert.assertTrue("Staging record should have empty data", results.get(1).getData().isEmpty());
    Assert.assertEquals(101L, (long) results.get(1).getSequenceNumber());

    // Third record: no-header (accepted - has data, permissive behavior)
    Assert.assertNotNull("No-header record should have data", results.get(2).getData());
    Assert.assertFalse("No-header record should have data", results.get(2).getData().isEmpty());
    Assert.assertEquals(102L, (long) results.get(2).getSequenceNumber());
    EasyMock.verify(mockConsumer);
  }

  @Test
  public void testFilteredFlagTracking()
  {
    // Test that filtered records are properly marked with filtered flag
    InDimFilter filter = new InDimFilter("environment", Collections.singletonList("production"), null);
    KafkaHeaderBasedFilteringConfig headerFilter = new KafkaHeaderBasedFilteringConfig(filter, null, null);

    recordSupplier = new KafkaRecordSupplier(mockConsumer, false, headerFilter);

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
    Assert.assertEquals("Should return 2 records (accepted + filtered)", 2, results.size());

    // Verify filtered flags
    Assert.assertFalse("Production record should not be filtered", results.get(0).isFiltered());
    Assert.assertTrue("Staging record should be filtered", results.get(1).isFiltered());

    // Verify data presence
    Assert.assertFalse("Production record should have data", results.get(0).getData().isEmpty());
    Assert.assertTrue("Filtered record should have empty data", results.get(1).getData().isEmpty());

    EasyMock.verify(mockConsumer);
  }

  @Test
  public void testInHeaderFilterMultipleValues()
  {
    // Test filtering with in filter (multiple values)
    InDimFilter filter = new InDimFilter("service", Arrays.asList("user-service", "payment-service"), null);
    KafkaHeaderBasedFilteringConfig headerFilter = new KafkaHeaderBasedFilteringConfig(filter, null, null);

    recordSupplier = new KafkaRecordSupplier(mockConsumer, false, headerFilter);

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

    Assert.assertEquals("Should return all records (accepted + filtered markers)", 3, results.size());

    // First record: user-service (accepted - has data)
    Assert.assertNotNull("User-service record should have data", results.get(0).getData());
    Assert.assertFalse("User-service record should have data", results.get(0).getData().isEmpty());
    Assert.assertEquals(100L, (long) results.get(0).getSequenceNumber());

    // Second record: payment-service (accepted - has data)
    Assert.assertNotNull("Payment-service record should have data", results.get(1).getData());
    Assert.assertFalse("Payment-service record should have data", results.get(1).getData().isEmpty());
    Assert.assertEquals(101L, (long) results.get(1).getSequenceNumber());

    // Third record: order-service (filtered - empty data for offset advancement marker)
    Assert.assertTrue("Order-service record should have empty data", results.get(2).getData().isEmpty());
    Assert.assertEquals(102L, (long) results.get(2).getSequenceNumber());
    EasyMock.verify(mockConsumer);
  }

  @Test
  public void testInFilterWithMultipleHeaders()
  {
    // Test InDimFilter with multiple possible values
    InDimFilter serviceFilter = new InDimFilter("service", Arrays.asList("user-service", "payment-service"), null);
    KafkaHeaderBasedFilteringConfig headerFilter = new KafkaHeaderBasedFilteringConfig(serviceFilter, null, null);

    recordSupplier = new KafkaRecordSupplier(mockConsumer, false, headerFilter);

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

    Assert.assertEquals("Should return all records (accepted + filtered markers)", 3, results.size());

    // First record: user-service (accepted - has data)
    Assert.assertNotNull("User-service record should have data", results.get(0).getData());
    Assert.assertFalse("User-service record should have data", results.get(0).getData().isEmpty());
    Assert.assertEquals(100L, (long) results.get(0).getSequenceNumber());

    // Second record: payment-service (accepted - has data)
    Assert.assertNotNull("Payment-service record should have data", results.get(1).getData());
    Assert.assertFalse("Payment-service record should have data", results.get(1).getData().isEmpty());
    Assert.assertEquals(101L, (long) results.get(1).getSequenceNumber());

    // Third record: order-service (filtered - empty data for offset advancement marker)
    Assert.assertTrue("Order-service record should have empty data", results.get(2).getData().isEmpty());
    Assert.assertEquals(102L, (long) results.get(2).getSequenceNumber());
    EasyMock.verify(mockConsumer);
  }

  @Test
  public void testMultiplePolls()
  {
    // Test that statistics accumulate across multiple polls
    InDimFilter filter = new InDimFilter("environment", Collections.singletonList("production"), null);
    KafkaHeaderBasedFilteringConfig headerFilter = new KafkaHeaderBasedFilteringConfig(filter, null, null);

    recordSupplier = new KafkaRecordSupplier(mockConsumer, false, headerFilter);

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

    Assert.assertEquals("First poll should return 2 records (accepted + filtered marker)", 2, results1.size());
    Assert.assertNotNull("Production record should have data", results1.get(0).getData());
    Assert.assertFalse("Production record should have data", results1.get(0).getData().isEmpty());
    Assert.assertTrue("Staging record should have empty data", results1.get(1).getData().isEmpty());

    List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> results2 =
        recordSupplier.poll(1000);

    Assert.assertEquals("Second poll should return 2 records (accepted + filtered marker)", 2, results2.size());
    Assert.assertNotNull("Production record should have data", results2.get(0).getData());
    Assert.assertFalse("Production record should have data", results2.get(0).getData().isEmpty());
    Assert.assertTrue("Staging record should have empty data", results2.get(1).getData().isEmpty());
    EasyMock.verify(mockConsumer);
  }

  @Test
  public void testEmptyPoll()
  {
    // Test that empty polls don't affect statistics
    InDimFilter filter = new InDimFilter("environment", Collections.singletonList("production"), null);
    KafkaHeaderBasedFilteringConfig headerFilter = new KafkaHeaderBasedFilteringConfig(filter, null, null);

    recordSupplier = new KafkaRecordSupplier(mockConsumer, false, headerFilter);

    EasyMock.expect(mockConsumer.poll(EasyMock.anyObject(Duration.class)))
        .andReturn(createConsumerRecords(Collections.emptyList()));
    EasyMock.replay(mockConsumer);

    List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> results =
        recordSupplier.poll(1000);

    Assert.assertEquals("Empty poll should return empty list", 0, results.size());
    EasyMock.verify(mockConsumer);
  }

  @Test
  public void testAllRecordsFilteredStillAdvanceOffsets()
  {
    // CRITICAL TEST: Verify that when ALL records are filtered out, we still return
    // filtered record markers to prevent infinite loop
    InDimFilter filter = new InDimFilter("environment", Collections.singletonList("production"), null);
    KafkaHeaderBasedFilteringConfig headerFilter = new KafkaHeaderBasedFilteringConfig(filter, null, null);

    recordSupplier = new KafkaRecordSupplier(mockConsumer, false, headerFilter);

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
    Assert.assertEquals("Should return filtered record markers for offset advancement", 3, results.size());

    // Verify that all returned records have filtered record markers
    for (OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity> result : results) {
      Assert.assertTrue("Filtered record should have empty data", result.getData().isEmpty());
    }

    // Verify offsets are correct
    Assert.assertEquals(100L, (long) results.get(0).getSequenceNumber());
    Assert.assertEquals(101L, (long) results.get(1).getSequenceNumber());
    Assert.assertEquals(102L, (long) results.get(2).getSequenceNumber());

    EasyMock.verify(mockConsumer);
  }

  @Test
  public void testMixedFilteredAndAcceptedRecords()
  {
    // Test that mix of filtered and accepted records works correctly
    InDimFilter filter = new InDimFilter("environment", Collections.singletonList("production"), null);
    KafkaHeaderBasedFilteringConfig headerFilter = new KafkaHeaderBasedFilteringConfig(filter, null, null);

    recordSupplier = new KafkaRecordSupplier(mockConsumer, false, headerFilter);

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

    Assert.assertEquals("Should return all records (accepted + filtered markers)", 3, results.size());

    // First record: accepted (has data)
    Assert.assertNotNull("Accepted record should have data", results.get(0).getData());
    Assert.assertEquals(100L, (long) results.get(0).getSequenceNumber());

    // Second record: filtered (empty data for offset advancement marker)
    Assert.assertTrue("Filtered record should have empty data", results.get(1).getData().isEmpty());
    Assert.assertEquals(101L, (long) results.get(1).getSequenceNumber());

    // Third record: accepted (has data)
    Assert.assertNotNull("Accepted record should have data", results.get(2).getData());
    Assert.assertEquals(102L, (long) results.get(2).getSequenceNumber());

    EasyMock.verify(mockConsumer);
  }

  @Test
  public void testMultiTopic()
  {
    // Test header filtering with multi-topic configuration
    InDimFilter filter = new InDimFilter("environment", Collections.singletonList("production"), null);
    KafkaHeaderBasedFilteringConfig headerFilter = new KafkaHeaderBasedFilteringConfig(filter, null, null);

    recordSupplier = new KafkaRecordSupplier(mockConsumer, true, headerFilter); // multiTopic = true

    ConsumerRecord<byte[], byte[]> topic1Record = createRecord("topic1", 0, 100L,
        headers("environment", "production"));
    ConsumerRecord<byte[], byte[]> topic2Record = createRecord("topic2", 0, 101L,
        headers("environment", "staging"));

    EasyMock.expect(mockConsumer.poll(EasyMock.anyObject(Duration.class)))
        .andReturn(createConsumerRecords(Arrays.asList(topic1Record, topic2Record)));
    EasyMock.replay(mockConsumer);

    List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> results =
        recordSupplier.poll(1000);

    Assert.assertEquals("Should return both records (accepted + filtered marker)", 2, results.size());

    // First record: accepted
    Assert.assertNotNull("Production record should have data", results.get(0).getData());
    Assert.assertEquals("topic1", results.get(0).getStream());
    Assert.assertTrue("Should be multi-topic partition",
        results.get(0).getPartitionId().isMultiTopicPartition());

    // Second record: filtered marker
    Assert.assertTrue("Staging record should have empty data", results.get(1).getData().isEmpty());
    Assert.assertEquals("topic2", results.get(1).getStream());

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
