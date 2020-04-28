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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.curator.test.TestingCluster;
import org.apache.druid.indexing.kafka.test.TestBroker;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.TestHelper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class KafkaRecordSupplierTest
{

  private static String topic = "topic";
  private static long poll_timeout_millis = 1000;
  private static int pollRetry = 5;
  private static int topicPosFix = 0;
  private static final ObjectMapper OBJECT_MAPPER = TestHelper.makeJsonMapper();

  private static TestingCluster zkServer;
  private static TestBroker kafkaServer;

  private List<ProducerRecord<byte[], byte[]>> records;


  private static List<ProducerRecord<byte[], byte[]>> generateRecords(String topic)
  {
    return ImmutableList.of(
        new ProducerRecord<>(topic, 0, null, jb("2008", "a", "y", "10", "20.0", "1.0")),
        new ProducerRecord<>(topic, 0, null, jb("2009", "b", "y", "10", "20.0", "1.0")),
        new ProducerRecord<>(topic, 0, null, jb("2010", "c", "y", "10", "20.0", "1.0")),
        new ProducerRecord<>(topic, 0, null, jb("2011", "d", "y", "10", "20.0", "1.0")),
        new ProducerRecord<>(topic, 0, null, jb("2011", "e", "y", "10", "20.0", "1.0")),
        new ProducerRecord<>(topic, 0, null, jb("246140482-04-24T15:36:27.903Z", "x", "z", "10", "20.0", "1.0")),
        new ProducerRecord<>(topic, 0, null, StringUtils.toUtf8("unparseable")),
        new ProducerRecord<>(topic, 0, null, StringUtils.toUtf8("unparseable2")),
        new ProducerRecord<>(topic, 0, null, null),
        new ProducerRecord<>(topic, 0, null, jb("2013", "f", "y", "10", "20.0", "1.0")),
        new ProducerRecord<>(topic, 0, null, jb("2049", "f", "y", "notanumber", "20.0", "1.0")),
        new ProducerRecord<>(topic, 1, null, jb("2049", "f", "y", "10", "notanumber", "1.0")),
        new ProducerRecord<>(topic, 1, null, jb("2049", "f", "y", "10", "20.0", "notanumber")),
        new ProducerRecord<>(topic, 1, null, jb("2012", "g", "y", "10", "20.0", "1.0")),
        new ProducerRecord<>(topic, 1, null, jb("2011", "h", "y", "10", "20.0", "1.0"))
    );
  }

  private static byte[] jb(String timestamp, String dim1, String dim2, String dimLong, String dimFloat, String met1)
  {
    try {
      return new ObjectMapper().writeValueAsBytes(
          ImmutableMap.builder()
                      .put("timestamp", timestamp)
                      .put("dim1", dim1)
                      .put("dim2", dim2)
                      .put("dimLong", dimLong)
                      .put("dimFloat", dimFloat)
                      .put("met1", met1)
                      .build()
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static String getTopicName()
  {
    return "topic-" + topicPosFix++;
  }

  private List<OrderedPartitionableRecord<Integer, Long>> createOrderedPartitionableRecords()
  {
    Map<Integer, Long> partitionToOffset = new HashMap<>();
    return records.stream().map(r -> {
      long offset = 0;
      if (partitionToOffset.containsKey(r.partition())) {
        offset = partitionToOffset.get(r.partition());
        partitionToOffset.put(r.partition(), offset + 1);
      } else {
        partitionToOffset.put(r.partition(), 1L);
      }
      return new OrderedPartitionableRecord<>(
          topic,
          r.partition(),
          offset,
          r.value() == null ? null : Collections.singletonList(r.value())
      );
    }).collect(Collectors.toList());
  }

  public static class TestKafkaDeserializer implements Deserializer<byte[]>
  {
    @Override
    public void configure(Map<String, ?> map, boolean b)
    {

    }

    @Override
    public void close()
    {

    }

    @Override
    public byte[] deserialize(String topic, byte[] data)
    {
      return data;
    }
  }

  @BeforeClass
  public static void setupClass() throws Exception
  {
    zkServer = new TestingCluster(1);
    zkServer.start();

    kafkaServer = new TestBroker(
        zkServer.getConnectString(),
        null,
        1,
        ImmutableMap.of("num.partitions", "2")
    );
    kafkaServer.start();

  }

  @Before
  public void setupTest()
  {
    topic = getTopicName();
    records = generateRecords(topic);
  }

  @AfterClass
  public static void tearDownClass() throws Exception
  {
    kafkaServer.close();
    kafkaServer = null;

    zkServer.stop();
    zkServer = null;
  }

  @Test
  public void testSupplierSetup() throws ExecutionException, InterruptedException
  {

    // Insert data
    insertData();

    Set<StreamPartition<Integer>> partitions = ImmutableSet.of(
        StreamPartition.of(topic, 0),
        StreamPartition.of(topic, 1)
    );

    KafkaRecordSupplier recordSupplier = new KafkaRecordSupplier(
        kafkaServer.consumerProperties(), OBJECT_MAPPER);

    Assert.assertTrue(recordSupplier.getAssignment().isEmpty());

    recordSupplier.assign(partitions);

    Assert.assertEquals(partitions, recordSupplier.getAssignment());
    Assert.assertEquals(ImmutableSet.of(0, 1), recordSupplier.getPartitionIds(topic));

    recordSupplier.close();
  }

  @Test
  public void testSupplierSetupCustomDeserializer() throws ExecutionException, InterruptedException
  {

    // Insert data
    insertData();

    Set<StreamPartition<Integer>> partitions = ImmutableSet.of(
        StreamPartition.of(topic, 0),
        StreamPartition.of(topic, 1)
    );

    Map<String, Object> properties = kafkaServer.consumerProperties();
    properties.put("key.deserializer", KafkaRecordSupplierTest.TestKafkaDeserializer.class.getName());
    properties.put("value.deserializer", KafkaRecordSupplierTest.TestKafkaDeserializer.class.getName());

    KafkaRecordSupplier recordSupplier = new KafkaRecordSupplier(
        properties,
        OBJECT_MAPPER
    );

    Assert.assertTrue(recordSupplier.getAssignment().isEmpty());

    recordSupplier.assign(partitions);

    Assert.assertEquals(partitions, recordSupplier.getAssignment());
    Assert.assertEquals(ImmutableSet.of(0, 1), recordSupplier.getPartitionIds(topic));

    recordSupplier.close();
  }

  @Test
  public void testPollCustomDeserializer() throws InterruptedException, ExecutionException
  {

    // Insert data
    insertData();

    Set<StreamPartition<Integer>> partitions = ImmutableSet.of(
        StreamPartition.of(topic, 0),
        StreamPartition.of(topic, 1)
    );

    Map<String, Object> properties = kafkaServer.consumerProperties();
    properties.put("key.deserializer", KafkaRecordSupplierTest.TestKafkaDeserializer.class.getName());
    properties.put("value.deserializer", KafkaRecordSupplierTest.TestKafkaDeserializer.class.getName());

    KafkaRecordSupplier recordSupplier = new KafkaRecordSupplier(
        properties,
        OBJECT_MAPPER
    );

    recordSupplier.assign(partitions);
    recordSupplier.seekToEarliest(partitions);

    List<OrderedPartitionableRecord<Integer, Long>> initialRecords = new ArrayList<>(createOrderedPartitionableRecords());

    List<OrderedPartitionableRecord<Integer, Long>> polledRecords = recordSupplier.poll(poll_timeout_millis);
    for (int i = 0; polledRecords.size() != initialRecords.size() && i < pollRetry; i++) {
      polledRecords.addAll(recordSupplier.poll(poll_timeout_millis));
      Thread.sleep(200);
    }

    Assert.assertEquals(partitions, recordSupplier.getAssignment());
    Assert.assertEquals(initialRecords.size(), polledRecords.size());
    Assert.assertTrue(initialRecords.containsAll(polledRecords));

    recordSupplier.close();
  }

  @Test
  public void testPoll() throws InterruptedException, ExecutionException
  {

    // Insert data
    insertData();

    Set<StreamPartition<Integer>> partitions = ImmutableSet.of(
        StreamPartition.of(topic, 0),
        StreamPartition.of(topic, 1)
    );

    KafkaRecordSupplier recordSupplier = new KafkaRecordSupplier(
        kafkaServer.consumerProperties(), OBJECT_MAPPER);

    recordSupplier.assign(partitions);
    recordSupplier.seekToEarliest(partitions);

    List<OrderedPartitionableRecord<Integer, Long>> initialRecords = new ArrayList<>(createOrderedPartitionableRecords());

    List<OrderedPartitionableRecord<Integer, Long>> polledRecords = recordSupplier.poll(poll_timeout_millis);
    for (int i = 0; polledRecords.size() != initialRecords.size() && i < pollRetry; i++) {
      polledRecords.addAll(recordSupplier.poll(poll_timeout_millis));
      Thread.sleep(200);
    }

    Assert.assertEquals(partitions, recordSupplier.getAssignment());
    Assert.assertEquals(initialRecords.size(), polledRecords.size());
    Assert.assertTrue(initialRecords.containsAll(polledRecords));

    recordSupplier.close();
  }


  @Test
  public void testPollAfterMoreDataAdded() throws InterruptedException, ExecutionException
  {
    // Insert data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      kafkaProducer.initTransactions();
      kafkaProducer.beginTransaction();
      for (ProducerRecord<byte[], byte[]> record : records.subList(0, 13)) {
        kafkaProducer.send(record).get();
      }
      kafkaProducer.commitTransaction();
    }

    Set<StreamPartition<Integer>> partitions = ImmutableSet.of(
        StreamPartition.of(topic, 0),
        StreamPartition.of(topic, 1)
    );


    KafkaRecordSupplier recordSupplier = new KafkaRecordSupplier(
        kafkaServer.consumerProperties(), OBJECT_MAPPER);

    recordSupplier.assign(partitions);
    recordSupplier.seekToEarliest(partitions);

    List<OrderedPartitionableRecord<Integer, Long>> polledRecords = recordSupplier.poll(poll_timeout_millis);
    for (int i = 0; polledRecords.size() != 13 && i < pollRetry; i++) {
      polledRecords.addAll(recordSupplier.poll(poll_timeout_millis));
      Thread.sleep(200);
    }

    // Insert data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      kafkaProducer.initTransactions();
      kafkaProducer.beginTransaction();
      for (ProducerRecord<byte[], byte[]> record : records.subList(13, 15)) {
        kafkaProducer.send(record).get();
      }
      kafkaProducer.commitTransaction();
    }


    for (int i = 0; polledRecords.size() != records.size() && i < pollRetry; i++) {
      polledRecords.addAll(recordSupplier.poll(poll_timeout_millis));
      Thread.sleep(200);
    }

    List<OrderedPartitionableRecord<Integer, Long>> initialRecords = createOrderedPartitionableRecords();

    Assert.assertEquals(records.size(), polledRecords.size());
    Assert.assertEquals(partitions, recordSupplier.getAssignment());

    final int initialRecordsPartition0Size = initialRecords.stream()
                                                           .filter(r -> r.getPartitionId().equals(0))
                                                           .collect(Collectors.toSet())
                                                           .size();
    final int initialRecordsPartition1Size = initialRecords.stream()
                                                           .filter(r -> r.getPartitionId().equals(1))
                                                           .collect(Collectors.toSet())
                                                           .size();

    final int polledRecordsPartition0Size = polledRecords.stream()
                                                         .filter(r -> r.getPartitionId().equals(0))
                                                         .collect(Collectors.toSet())
                                                         .size();
    final int polledRecordsPartition1Size = polledRecords.stream()
                                                         .filter(r -> r.getPartitionId().equals(1))
                                                         .collect(Collectors.toSet())
                                                         .size();

    Assert.assertEquals(initialRecordsPartition0Size, polledRecordsPartition0Size);
    Assert.assertEquals(initialRecordsPartition1Size, polledRecordsPartition1Size);

    recordSupplier.close();
  }

  @Test
  public void testSeek() throws InterruptedException, ExecutionException
  {
    // Insert data
    insertData();

    StreamPartition<Integer> partition0 = StreamPartition.of(topic, 0);
    StreamPartition<Integer> partition1 = StreamPartition.of(topic, 1);

    Set<StreamPartition<Integer>> partitions = ImmutableSet.of(
        StreamPartition.of(topic, 0),
        StreamPartition.of(topic, 1)
    );

    KafkaRecordSupplier recordSupplier = new KafkaRecordSupplier(
        kafkaServer.consumerProperties(), OBJECT_MAPPER);

    recordSupplier.assign(partitions);
    recordSupplier.seekToEarliest(partitions);

    Assert.assertEquals(0L, (long) recordSupplier.getEarliestSequenceNumber(partition0));
    Assert.assertEquals(0L, (long) recordSupplier.getEarliestSequenceNumber(partition1));

    recordSupplier.seek(partition0, 2L);
    recordSupplier.seek(partition1, 2L);

    List<OrderedPartitionableRecord<Integer, Long>> initialRecords = createOrderedPartitionableRecords();

    List<OrderedPartitionableRecord<Integer, Long>> polledRecords = recordSupplier.poll(poll_timeout_millis);
    for (int i = 0; polledRecords.size() != 11 && i < pollRetry; i++) {
      polledRecords.addAll(recordSupplier.poll(poll_timeout_millis));
      Thread.sleep(200);
    }


    Assert.assertEquals(11, polledRecords.size());
    Assert.assertTrue(initialRecords.containsAll(polledRecords));


    recordSupplier.close();

  }

  @Test
  public void testSeekToLatest() throws InterruptedException, ExecutionException
  {
    // Insert data
    insertData();

    StreamPartition<Integer> partition0 = StreamPartition.of(topic, 0);
    StreamPartition<Integer> partition1 = StreamPartition.of(topic, 1);

    Set<StreamPartition<Integer>> partitions = ImmutableSet.of(
        StreamPartition.of(topic, 0),
        StreamPartition.of(topic, 1)
    );

    KafkaRecordSupplier recordSupplier = new KafkaRecordSupplier(
        kafkaServer.consumerProperties(), OBJECT_MAPPER);

    recordSupplier.assign(partitions);
    recordSupplier.seekToEarliest(partitions);

    Assert.assertEquals(0L, (long) recordSupplier.getEarliestSequenceNumber(partition0));
    Assert.assertEquals(0L, (long) recordSupplier.getEarliestSequenceNumber(partition1));

    recordSupplier.seekToLatest(partitions);
    List<OrderedPartitionableRecord<Integer, Long>> polledRecords = recordSupplier.poll(poll_timeout_millis);

    Assert.assertEquals(Collections.emptyList(), polledRecords);
    recordSupplier.close();
  }

  @Test(expected = IllegalStateException.class)
  public void testSeekUnassigned() throws InterruptedException, ExecutionException
  {
    // Insert data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : records) {
        kafkaProducer.send(record).get();
      }
    }

    StreamPartition<Integer> partition0 = StreamPartition.of(topic, 0);
    StreamPartition<Integer> partition1 = StreamPartition.of(topic, 1);

    Set<StreamPartition<Integer>> partitions = ImmutableSet.of(
        StreamPartition.of(topic, 0)
    );

    KafkaRecordSupplier recordSupplier = new KafkaRecordSupplier(
        kafkaServer.consumerProperties(), OBJECT_MAPPER);

    recordSupplier.assign(partitions);

    Assert.assertEquals(0, (long) recordSupplier.getEarliestSequenceNumber(partition0));

    recordSupplier.seekToEarliest(Collections.singleton(partition1));

    recordSupplier.close();
  }

  @Test
  public void testPosition() throws ExecutionException, InterruptedException
  {
    // Insert data
    insertData();

    StreamPartition<Integer> partition0 = StreamPartition.of(topic, 0);
    StreamPartition<Integer> partition1 = StreamPartition.of(topic, 1);

    Set<StreamPartition<Integer>> partitions = ImmutableSet.of(
        StreamPartition.of(topic, 0),
        StreamPartition.of(topic, 1)
    );

    KafkaRecordSupplier recordSupplier = new KafkaRecordSupplier(
        kafkaServer.consumerProperties(), OBJECT_MAPPER);

    recordSupplier.assign(partitions);
    recordSupplier.seekToEarliest(partitions);

    Assert.assertEquals(0L, (long) recordSupplier.getPosition(partition0));
    Assert.assertEquals(0L, (long) recordSupplier.getPosition(partition1));

    recordSupplier.seek(partition0, 4L);
    recordSupplier.seek(partition1, 5L);

    Assert.assertEquals(4L, (long) recordSupplier.getPosition(partition0));
    Assert.assertEquals(5L, (long) recordSupplier.getPosition(partition1));

    recordSupplier.seekToEarliest(Collections.singleton(partition0));
    Assert.assertEquals(0L, (long) recordSupplier.getPosition(partition0));

    recordSupplier.seekToLatest(Collections.singleton(partition0));
    Assert.assertEquals(12L, (long) recordSupplier.getPosition(partition0));

    long prevPos = recordSupplier.getPosition(partition0);
    recordSupplier.getEarliestSequenceNumber(partition0);
    Assert.assertEquals(prevPos, (long) recordSupplier.getPosition(partition0));

    recordSupplier.getLatestSequenceNumber(partition0);
    Assert.assertEquals(prevPos, (long) recordSupplier.getPosition(partition0));


    recordSupplier.close();
  }

  @Test
  public void getLatestSequenceNumberWhenPartitionIsEmptyAndUseEarliestOffsetShouldReturnsValidNonNull()
  {
    KafkaRecordSupplier recordSupplier = new KafkaRecordSupplier(
        kafkaServer.consumerProperties(), OBJECT_MAPPER);
    StreamPartition<Integer> streamPartition = StreamPartition.of(topic, 0);
    Set<StreamPartition<Integer>> partitions = ImmutableSet.of(streamPartition);
    recordSupplier.assign(partitions);
    recordSupplier.seekToEarliest(partitions);
    Assert.assertEquals(new Long(0), recordSupplier.getLatestSequenceNumber(streamPartition));
  }

  @Test
  public void getEarliestSequenceNumberWhenPartitionIsEmptyAndUseEarliestOffsetShouldReturnsValidNonNull()
  {
    KafkaRecordSupplier recordSupplier = new KafkaRecordSupplier(
        kafkaServer.consumerProperties(), OBJECT_MAPPER);
    StreamPartition<Integer> streamPartition = StreamPartition.of(topic, 0);
    Set<StreamPartition<Integer>> partitions = ImmutableSet.of(streamPartition);
    recordSupplier.assign(partitions);
    recordSupplier.seekToEarliest(partitions);
    Assert.assertEquals(new Long(0), recordSupplier.getEarliestSequenceNumber(streamPartition));
  }

  @Test
  public void getLatestSequenceNumberWhenPartitionIsEmptyAndUseLatestOffsetShouldReturnsValidNonNull()
  {
    KafkaRecordSupplier recordSupplier = new KafkaRecordSupplier(
        kafkaServer.consumerProperties(), OBJECT_MAPPER);
    StreamPartition<Integer> streamPartition = StreamPartition.of(topic, 0);
    Set<StreamPartition<Integer>> partitions = ImmutableSet.of(streamPartition);
    recordSupplier.assign(partitions);
    recordSupplier.seekToLatest(partitions);
    Assert.assertEquals(new Long(0), recordSupplier.getLatestSequenceNumber(streamPartition));
  }

  @Test
  public void getEarliestSequenceNumberWhenPartitionIsEmptyAndUseLatestOffsetShouldReturnsValidNonNull()
  {
    KafkaRecordSupplier recordSupplier = new KafkaRecordSupplier(
        kafkaServer.consumerProperties(), OBJECT_MAPPER);
    StreamPartition<Integer> streamPartition = StreamPartition.of(topic, 0);
    Set<StreamPartition<Integer>> partitions = ImmutableSet.of(streamPartition);
    recordSupplier.assign(partitions);
    recordSupplier.seekToLatest(partitions);
    Assert.assertEquals(new Long(0), recordSupplier.getEarliestSequenceNumber(streamPartition));
  }

  private void insertData() throws ExecutionException, InterruptedException
  {
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      kafkaProducer.initTransactions();
      kafkaProducer.beginTransaction();
      for (ProducerRecord<byte[], byte[]> record : records) {
        kafkaProducer.send(record).get();
      }
      kafkaProducer.commitTransaction();
    }
  }

}
