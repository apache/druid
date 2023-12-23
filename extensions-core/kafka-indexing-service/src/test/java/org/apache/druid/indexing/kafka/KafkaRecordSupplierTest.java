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
import com.google.common.collect.Sets;
import org.apache.curator.test.TestingCluster;
import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.data.input.kafka.KafkaTopicPartition;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorIOConfig;
import org.apache.druid.indexing.kafka.test.TestBroker;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.metrics.Monitor;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.metadata.DynamicConfigProvider;
import org.apache.druid.metadata.MapStringDynamicConfigProvider;
import org.apache.druid.segment.TestHelper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
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
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class KafkaRecordSupplierTest
{

  private static final String ADDITIONAL_PARAMETER = "additional.parameter";
  private static final long POLL_TIMEOUT_MILLIS = 1000;
  private static final int POLL_RETRY = 5;
  private static final ObjectMapper OBJECT_MAPPER = TestHelper.makeJsonMapper();

  private static KafkaTopicPartition PARTITION_0 = new KafkaTopicPartition(false, null, 0);
  private static KafkaTopicPartition PARTITION_1 = new KafkaTopicPartition(false, null, 1);

  private static String TOPIC = "topic";
  private static int TOPIC_POS_FIX = 0;
  private static TestingCluster ZK_SERVER;
  private static TestBroker KAFKA_SERVER;

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

  private static String nextTopicName()
  {
    return "topic-" + TOPIC_POS_FIX++;
  }

  private List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> createOrderedPartitionableRecords()
  {
    Map<KafkaTopicPartition, Long> partitionToOffset = new HashMap<>();
    return records.stream().map(r -> {
      long offset = 0;
      KafkaTopicPartition tp = new KafkaTopicPartition(false, r.topic(), r.partition());
      if (partitionToOffset.containsKey(tp)) {
        offset = partitionToOffset.get(tp);
        partitionToOffset.put(tp, offset + 1);
      } else {
        partitionToOffset.put(tp, 1L);
      }
      return new OrderedPartitionableRecord<>(
          TOPIC,
          tp,
          offset,
          r.value() == null ? null : Collections.singletonList(new KafkaRecordEntity(
              new ConsumerRecord<>(r.topic(), r.partition(), offset, r.key(), r.value())
          ))
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
    public byte[] deserialize(String TOPIC, byte[] data)
    {
      return data;
    }
  }


  public static class TestKafkaDeserializerRequiresParameter implements Deserializer<byte[]>
  {
    @Override
    public void configure(Map<String, ?> map, boolean b)
    {
      if (!map.containsKey("additional.parameter")) {
        throw new IllegalStateException("require additional.parameter configured");
      }
    }

    @Override
    public void close()
    {

    }

    @Override
    public byte[] deserialize(String TOPIC, byte[] data)
    {
      return data;
    }
  }

  @BeforeClass
  public static void setupClass() throws Exception
  {
    ZK_SERVER = new TestingCluster(1);
    ZK_SERVER.start();

    KAFKA_SERVER = new TestBroker(
        ZK_SERVER.getConnectString(),
        null,
        1,
        ImmutableMap.of("num.partitions", "2")
    );
    KAFKA_SERVER.start();

  }

  @Before
  public void setupTest()
  {
    TOPIC = nextTopicName();
    records = generateRecords(TOPIC);
  }

  @AfterClass
  public static void tearDownClass() throws Exception
  {
    KAFKA_SERVER.close();
    KAFKA_SERVER = null;

    ZK_SERVER.stop();
    ZK_SERVER = null;
  }

  @Test
  public void testSupplierSetup() throws ExecutionException, InterruptedException
  {

    // Insert data
    insertData();

    Set<StreamPartition<KafkaTopicPartition>> partitions = ImmutableSet.of(
        StreamPartition.of(TOPIC, PARTITION_0),
        StreamPartition.of(TOPIC, PARTITION_1)
    );

    KafkaRecordSupplier recordSupplier = new KafkaRecordSupplier(
        KAFKA_SERVER.consumerProperties(), OBJECT_MAPPER, null, false);

    Assert.assertTrue(recordSupplier.getAssignment().isEmpty());

    recordSupplier.assign(partitions);

    Assert.assertEquals(partitions, recordSupplier.getAssignment());
    Assert.assertEquals(ImmutableSet.of(PARTITION_0, PARTITION_1),
                        recordSupplier.getPartitionIds(TOPIC));

    recordSupplier.close();
  }

  @Test
  public void testMultiTopicSupplierSetup() throws ExecutionException, InterruptedException
  {
    // Insert data into TOPIC
    insertData();

    // Insert data into other topic
    String otherTopic = nextTopicName();
    records = generateRecords(otherTopic);
    insertData();

    KafkaRecordSupplier recordSupplier = new KafkaRecordSupplier(
        KAFKA_SERVER.consumerProperties(), OBJECT_MAPPER, null, true);

    String stream = Pattern.quote(TOPIC) + "|" + Pattern.quote(otherTopic);
    Set<KafkaTopicPartition> partitions = recordSupplier.getPartitionIds(stream);
    Set<KafkaTopicPartition> diff = Sets.difference(
        ImmutableSet.of(
            new KafkaTopicPartition(true, TOPIC, 0),
            new KafkaTopicPartition(true, TOPIC, 1),
            new KafkaTopicPartition(true, otherTopic, 0),
            new KafkaTopicPartition(true, otherTopic, 1)
        ),
        partitions
    );
    Assert.assertEquals(diff.toString(), 0, diff.size());
  }

  @Test
  public void testSupplierSetupCustomDeserializer() throws ExecutionException, InterruptedException
  {

    // Insert data
    insertData();

    Set<StreamPartition<KafkaTopicPartition>> partitions = ImmutableSet.of(
        StreamPartition.of(TOPIC, PARTITION_0),
        StreamPartition.of(TOPIC, PARTITION_1)
    );

    Map<String, Object> properties = KAFKA_SERVER.consumerProperties();
    properties.put("key.deserializer", KafkaRecordSupplierTest.TestKafkaDeserializer.class.getName());
    properties.put("value.deserializer", KafkaRecordSupplierTest.TestKafkaDeserializer.class.getName());

    KafkaRecordSupplier recordSupplier = new KafkaRecordSupplier(
        properties,
        OBJECT_MAPPER,
        null,
        false
    );

    Assert.assertTrue(recordSupplier.getAssignment().isEmpty());

    recordSupplier.assign(partitions);

    Assert.assertEquals(partitions, recordSupplier.getAssignment());
    Assert.assertEquals(ImmutableSet.of(PARTITION_0, PARTITION_1),
                        recordSupplier.getPartitionIds(TOPIC));

    recordSupplier.close();
  }


  @Test
  public void testSupplierSetupCustomDeserializerRequiresParameter()
  {

    Map<String, Object> properties = KAFKA_SERVER.consumerProperties();
    properties.put("key.deserializer", KafkaRecordSupplierTest.TestKafkaDeserializerRequiresParameter.class.getName());
    properties.put("value.deserializer", KafkaRecordSupplierTest.TestKafkaDeserializerRequiresParameter.class.getName());
    properties.put(ADDITIONAL_PARAMETER, "stringValue");

    KafkaRecordSupplier recordSupplier = new KafkaRecordSupplier(
            properties,
            OBJECT_MAPPER,
            null,
            false
    );

    Assert.assertTrue(recordSupplier.getAssignment().isEmpty()); //just test recordSupplier is initiated
    recordSupplier.close();
  }

  @Test(expected = IllegalStateException.class)
  public void testSupplierSetupCustomDeserializerRequiresParameterButMissingIt()
  {

    Map<String, Object> properties = KAFKA_SERVER.consumerProperties();
    properties.put("key.deserializer", KafkaRecordSupplierTest.TestKafkaDeserializerRequiresParameter.class.getName());
    properties.put("value.deserializer", KafkaRecordSupplierTest.TestKafkaDeserializerRequiresParameter.class.getName());

    KafkaRecordSupplier recordSupplier = new KafkaRecordSupplier(
            properties,
            OBJECT_MAPPER,
            null,
            false
    );

    Assert.assertTrue(recordSupplier.getAssignment().isEmpty()); //just test recordSupplier is initiated
    recordSupplier.close();
  }

  @Test
  public void testPollCustomDeserializer() throws InterruptedException, ExecutionException
  {

    // Insert data
    insertData();

    Set<StreamPartition<KafkaTopicPartition>> partitions = ImmutableSet.of(
        StreamPartition.of(TOPIC, PARTITION_0),
        StreamPartition.of(TOPIC, PARTITION_1)
    );

    Map<String, Object> properties = KAFKA_SERVER.consumerProperties();
    properties.put("key.deserializer", KafkaRecordSupplierTest.TestKafkaDeserializer.class.getName());
    properties.put("value.deserializer", KafkaRecordSupplierTest.TestKafkaDeserializer.class.getName());

    KafkaRecordSupplier recordSupplier = new KafkaRecordSupplier(
        properties,
        OBJECT_MAPPER,
        null,
        false
    );

    recordSupplier.assign(partitions);
    recordSupplier.seekToEarliest(partitions);

    List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> initialRecords = new ArrayList<>(createOrderedPartitionableRecords());

    List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> polledRecords =
        recordSupplier.poll(POLL_TIMEOUT_MILLIS);
    for (int i = 0; polledRecords.size() != initialRecords.size() && i < POLL_RETRY; i++) {
      polledRecords.addAll(recordSupplier.poll(POLL_TIMEOUT_MILLIS));
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

    Set<StreamPartition<KafkaTopicPartition>> partitions = ImmutableSet.of(
        StreamPartition.of(TOPIC, PARTITION_0),
        StreamPartition.of(TOPIC, PARTITION_1)
    );

    KafkaRecordSupplier recordSupplier = new KafkaRecordSupplier(
        KAFKA_SERVER.consumerProperties(),
        OBJECT_MAPPER,
        null,
        false
    );

    final Monitor monitor = recordSupplier.monitor();
    monitor.start();
    recordSupplier.assign(partitions);
    recordSupplier.seekToEarliest(partitions);

    List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> initialRecords = new ArrayList<>(createOrderedPartitionableRecords());

    List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> polledRecords = recordSupplier.poll(POLL_TIMEOUT_MILLIS);
    for (int i = 0; polledRecords.size() != initialRecords.size() && i < POLL_RETRY; i++) {
      polledRecords.addAll(recordSupplier.poll(POLL_TIMEOUT_MILLIS));
      Thread.sleep(200);
    }

    Assert.assertEquals(partitions, recordSupplier.getAssignment());
    Assert.assertEquals(initialRecords.size(), polledRecords.size());
    Assert.assertTrue(initialRecords.containsAll(polledRecords));

    // Verify metrics
    final StubServiceEmitter emitter = new StubServiceEmitter("service", "host");
    Assert.assertTrue(monitor.monitor(emitter));
    emitter.verifyEmitted("kafka/consumer/bytesConsumed", 1);
    emitter.verifyEmitted("kafka/consumer/recordsConsumed", 1);

    recordSupplier.close();
    Assert.assertFalse(monitor.monitor(emitter));
  }


  @Test
  public void testPollAfterMoreDataAdded() throws InterruptedException, ExecutionException
  {
    // Insert data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = KAFKA_SERVER.newProducer()) {
      kafkaProducer.initTransactions();
      kafkaProducer.beginTransaction();
      for (ProducerRecord<byte[], byte[]> record : records.subList(0, 13)) {
        kafkaProducer.send(record).get();
      }
      kafkaProducer.commitTransaction();
    }

    Set<StreamPartition<KafkaTopicPartition>> partitions = ImmutableSet.of(
        StreamPartition.of(TOPIC, PARTITION_0),
        StreamPartition.of(TOPIC, PARTITION_1)
    );


    KafkaRecordSupplier recordSupplier = new KafkaRecordSupplier(
        KAFKA_SERVER.consumerProperties(), OBJECT_MAPPER, null, false);
    recordSupplier.assign(partitions);
    recordSupplier.seekToEarliest(partitions);

    List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> polledRecords = recordSupplier.poll(POLL_TIMEOUT_MILLIS);
    for (int i = 0; polledRecords.size() != 13 && i < POLL_RETRY; i++) {
      polledRecords.addAll(recordSupplier.poll(POLL_TIMEOUT_MILLIS));
      Thread.sleep(200);
    }

    // Insert data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = KAFKA_SERVER.newProducer()) {
      kafkaProducer.initTransactions();
      kafkaProducer.beginTransaction();
      for (ProducerRecord<byte[], byte[]> record : records.subList(13, 15)) {
        kafkaProducer.send(record).get();
      }
      kafkaProducer.commitTransaction();
    }


    for (int i = 0; polledRecords.size() != records.size() && i < POLL_RETRY; i++) {
      polledRecords.addAll(recordSupplier.poll(POLL_TIMEOUT_MILLIS));
      Thread.sleep(200);
    }

    List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> initialRecords = createOrderedPartitionableRecords();

    Assert.assertEquals(records.size(), polledRecords.size());
    Assert.assertEquals(partitions, recordSupplier.getAssignment());

    final int initialRecordsPartition0Size = initialRecords.stream()
                                                           .filter(r -> r.getPartitionId().partition() == 0)
                                                           .collect(Collectors.toSet())
                                                           .size();
    final int initialRecordsPartition1Size = initialRecords.stream()
                                                           .filter(r -> r.getPartitionId().partition() == 1)
                                                           .collect(Collectors.toSet())
                                                           .size();

    final int polledRecordsPartition0Size = polledRecords.stream()
                                                         .filter(r -> r.getPartitionId().partition() == 0)
                                                         .collect(Collectors.toSet())
                                                         .size();
    final int polledRecordsPartition1Size = polledRecords.stream()
                                                         .filter(r -> r.getPartitionId().partition() == 1)
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

    StreamPartition<KafkaTopicPartition> partition0 = StreamPartition.of(TOPIC, PARTITION_0);
    StreamPartition<KafkaTopicPartition> partition1 = StreamPartition.of(TOPIC, PARTITION_1);

    Set<StreamPartition<KafkaTopicPartition>> partitions = ImmutableSet.of(
        StreamPartition.of(TOPIC, PARTITION_0),
        StreamPartition.of(TOPIC, PARTITION_1)
    );

    KafkaRecordSupplier recordSupplier = new KafkaRecordSupplier(
        KAFKA_SERVER.consumerProperties(), OBJECT_MAPPER, null, false);

    recordSupplier.assign(partitions);
    recordSupplier.seekToEarliest(partitions);

    Assert.assertEquals(0L, (long) recordSupplier.getEarliestSequenceNumber(partition0));
    Assert.assertEquals(0L, (long) recordSupplier.getEarliestSequenceNumber(partition1));

    recordSupplier.seek(partition0, 2L);
    recordSupplier.seek(partition1, 2L);

    List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> initialRecords = createOrderedPartitionableRecords();

    List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> polledRecords = recordSupplier.poll(POLL_TIMEOUT_MILLIS);
    for (int i = 0; polledRecords.size() != 11 && i < POLL_RETRY; i++) {
      polledRecords.addAll(recordSupplier.poll(POLL_TIMEOUT_MILLIS));
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

    StreamPartition<KafkaTopicPartition> partition0 = StreamPartition.of(TOPIC, PARTITION_0);
    StreamPartition<KafkaTopicPartition> partition1 = StreamPartition.of(TOPIC, PARTITION_1);

    Set<StreamPartition<KafkaTopicPartition>> partitions = ImmutableSet.of(
        StreamPartition.of(TOPIC, PARTITION_0),
        StreamPartition.of(TOPIC, PARTITION_1)
    );

    KafkaRecordSupplier recordSupplier = new KafkaRecordSupplier(
        KAFKA_SERVER.consumerProperties(), OBJECT_MAPPER, null, false);

    recordSupplier.assign(partitions);
    recordSupplier.seekToEarliest(partitions);

    Assert.assertEquals(0L, (long) recordSupplier.getEarliestSequenceNumber(partition0));
    Assert.assertEquals(0L, (long) recordSupplier.getEarliestSequenceNumber(partition1));

    recordSupplier.seekToLatest(partitions);
    List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> polledRecords = recordSupplier.poll(POLL_TIMEOUT_MILLIS);

    Assert.assertEquals(Collections.emptyList(), polledRecords);
    recordSupplier.close();
  }

  @Test(expected = IllegalStateException.class)
  public void testSeekUnassigned() throws InterruptedException, ExecutionException
  {
    // Insert data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = KAFKA_SERVER.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : records) {
        kafkaProducer.send(record).get();
      }
    }

    StreamPartition<KafkaTopicPartition> partition0 = StreamPartition.of(TOPIC, PARTITION_0);
    StreamPartition<KafkaTopicPartition> partition1 = StreamPartition.of(TOPIC, PARTITION_1);

    Set<StreamPartition<KafkaTopicPartition>> partitions = ImmutableSet.of(
        StreamPartition.of(TOPIC, PARTITION_0)
    );

    KafkaRecordSupplier recordSupplier = new KafkaRecordSupplier(
        KAFKA_SERVER.consumerProperties(), OBJECT_MAPPER, null, false);

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

    StreamPartition<KafkaTopicPartition> partition0 = StreamPartition.of(TOPIC, PARTITION_0);
    StreamPartition<KafkaTopicPartition> partition1 = StreamPartition.of(TOPIC, PARTITION_1);

    Set<StreamPartition<KafkaTopicPartition>> partitions = ImmutableSet.of(
        StreamPartition.of(TOPIC, PARTITION_0),
        StreamPartition.of(TOPIC, PARTITION_1)
    );

    KafkaRecordSupplier recordSupplier = new KafkaRecordSupplier(
        KAFKA_SERVER.consumerProperties(), OBJECT_MAPPER, null, false);

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
        KAFKA_SERVER.consumerProperties(), OBJECT_MAPPER, null, false);
    StreamPartition<KafkaTopicPartition> streamPartition = StreamPartition.of(TOPIC, PARTITION_0);
    Set<StreamPartition<KafkaTopicPartition>> partitions = ImmutableSet.of(streamPartition);
    recordSupplier.assign(partitions);
    recordSupplier.seekToEarliest(partitions);
    Assert.assertEquals(new Long(0), recordSupplier.getLatestSequenceNumber(streamPartition));
  }

  @Test
  public void getEarliestSequenceNumberWhenPartitionIsEmptyAndUseEarliestOffsetShouldReturnsValidNonNull()
  {
    KafkaRecordSupplier recordSupplier = new KafkaRecordSupplier(
        KAFKA_SERVER.consumerProperties(), OBJECT_MAPPER, null, false);
    StreamPartition<KafkaTopicPartition> streamPartition = StreamPartition.of(TOPIC, PARTITION_0);
    Set<StreamPartition<KafkaTopicPartition>> partitions = ImmutableSet.of(streamPartition);
    recordSupplier.assign(partitions);
    recordSupplier.seekToEarliest(partitions);
    Assert.assertEquals(new Long(0), recordSupplier.getEarliestSequenceNumber(streamPartition));
  }

  @Test
  public void getLatestSequenceNumberWhenPartitionIsEmptyAndUseLatestOffsetShouldReturnsValidNonNull()
  {
    KafkaRecordSupplier recordSupplier = new KafkaRecordSupplier(
        KAFKA_SERVER.consumerProperties(), OBJECT_MAPPER, null, false);
    StreamPartition<KafkaTopicPartition> streamPartition = StreamPartition.of(TOPIC, PARTITION_0);
    Set<StreamPartition<KafkaTopicPartition>> partitions = ImmutableSet.of(streamPartition);
    recordSupplier.assign(partitions);
    recordSupplier.seekToLatest(partitions);
    Assert.assertEquals(new Long(0), recordSupplier.getLatestSequenceNumber(streamPartition));
  }

  @Test
  public void getEarliestSequenceNumberWhenPartitionIsEmptyAndUseLatestOffsetShouldReturnsValidNonNull()
  {
    KafkaRecordSupplier recordSupplier = new KafkaRecordSupplier(
        KAFKA_SERVER.consumerProperties(), OBJECT_MAPPER, null, false);
    StreamPartition<KafkaTopicPartition> streamPartition = StreamPartition.of(TOPIC, PARTITION_0);
    Set<StreamPartition<KafkaTopicPartition>> partitions = ImmutableSet.of(streamPartition);
    recordSupplier.assign(partitions);
    recordSupplier.seekToLatest(partitions);
    Assert.assertEquals(new Long(0), recordSupplier.getEarliestSequenceNumber(streamPartition));
  }

  @Test
  public void testAddConsumerPropertiesFromConfig()
  {
    DynamicConfigProvider dynamicConfigProvider = new MapStringDynamicConfigProvider(
        ImmutableMap.of("kafka.prop.2", "value.2", KafkaSupervisorIOConfig.TRUST_STORE_PASSWORD_KEY, "pwd2")
    );

    Properties properties = new Properties();

    Map<String, Object> consumerProperties = ImmutableMap.of(
        KafkaSupervisorIOConfig.TRUST_STORE_PASSWORD_KEY, "pwd1",
        "kafka.prop.1", "value.1",
        "druid.dynamic.config.provider", OBJECT_MAPPER.convertValue(dynamicConfigProvider, Map.class)
    );

    KafkaRecordSupplier.addConsumerPropertiesFromConfig(
        properties,
        OBJECT_MAPPER,
        consumerProperties
    );

    Assert.assertEquals(3, properties.size());
    Assert.assertEquals("value.1", properties.getProperty("kafka.prop.1"));
    Assert.assertEquals("value.2", properties.getProperty("kafka.prop.2"));
    Assert.assertEquals("pwd2", properties.getProperty(KafkaSupervisorIOConfig.TRUST_STORE_PASSWORD_KEY));
  }

  @Test
  public void testUseKafkaConsumerOverrides()
  {
    KafkaConsumer<byte[], byte[]> kafkaConsumer = KafkaRecordSupplier.getKafkaConsumer(
        OBJECT_MAPPER,
        KAFKA_SERVER.consumerProperties(),
        originalConsumerProperties -> {
          final Map<String, Object> newMap = new HashMap<>(originalConsumerProperties);
          newMap.put("client.id", "overrideConfigTest");
          return newMap;
        }
    );

    // We set a client ID via config override, it should appear in the metric name tags
    Map<MetricName, KafkaMetric> metrics = (Map<MetricName, KafkaMetric>) kafkaConsumer.metrics();
    for (MetricName metricName : metrics.keySet()) {
      Assert.assertEquals("overrideConfigTest", metricName.tags().get("client-id"));
      break;
    }
  }

  private void insertData() throws ExecutionException, InterruptedException
  {
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = KAFKA_SERVER.newProducer()) {
      kafkaProducer.initTransactions();
      kafkaProducer.beginTransaction();
      for (ProducerRecord<byte[], byte[]> record : records) {
        kafkaProducer.send(record).get();
      }
      kafkaProducer.commitTransaction();
    }
  }

}
