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

package org.apache.druid.indexing.rocketmq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.data.input.rocketmq.PartitionUtil;
import org.apache.druid.data.input.rocketmq.RocketMQRecordEntity;
import org.apache.druid.indexing.rocketmq.test.TestBroker;
import org.apache.druid.indexing.rocketmq.test.TestProducer;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.TestHelper;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
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
import java.util.stream.Collectors;

public class RocketMQRecordSupplierTest
{
  private static String topic = "topic";
  private static final String BROKER_NAME = "broker-a";
  private static long poll_timeout_millis = 1000;
  private static int pollRetry = 10;
  private static int topicPosFix = 0;
  private static final ObjectMapper OBJECT_MAPPER = TestHelper.makeJsonMapper();

  private static TestBroker rocketmqServer;

  private List<Pair<MessageQueue, Message>> records;


  private static List<Pair<MessageQueue, Message>> generateRecords(String topic)
  {
    return ImmutableList.of(
        new Pair<>(new MessageQueue(topic, BROKER_NAME, 0), new Message(topic, "TagA", jb("2008", "a", "y", "10", "20.0", "1.0"))),
        new Pair<>(new MessageQueue(topic, BROKER_NAME, 0), new Message(topic, "TagA", jb("2009", "b", "y", "10", "20.0", "1.0"))),
        new Pair<>(new MessageQueue(topic, BROKER_NAME, 0), new Message(topic, "TagA", jb("2010", "c", "y", "10", "20.0", "1.0"))),
        new Pair<>(new MessageQueue(topic, BROKER_NAME, 0), new Message(topic, "TagA", jb("2011", "d", "y", "10", "20.0", "1.0"))),
        new Pair<>(new MessageQueue(topic, BROKER_NAME, 0), new Message(topic, "TagA", jb("2011", "e", "y", "10", "20.0", "1.0"))),
        new Pair<>(new MessageQueue(topic, BROKER_NAME, 0), new Message(topic, "TagA", jb("246140482-04-24T15:36:27.903Z", "x", "z", "10", "20.0", "1.0"))),
        new Pair<>(new MessageQueue(topic, BROKER_NAME, 0), new Message(topic, "TagA", StringUtils.toUtf8("unparseable"))),
        new Pair<>(new MessageQueue(topic, BROKER_NAME, 0), new Message(topic, "TagA", StringUtils.toUtf8("unparseable2"))),
        new Pair<>(new MessageQueue(topic, BROKER_NAME, 0), new Message(topic, "TagA", jb("2013", "f", "y", "10", "20.0", "1.0"))),
        new Pair<>(new MessageQueue(topic, BROKER_NAME, 0), new Message(topic, "TagA", jb("2049", "f", "y", "notanumber", "20.0", "1.0"))),
        new Pair<>(new MessageQueue(topic, BROKER_NAME, 1), new Message(topic, "TagA", jb("2049", "f", "y", "10", "notanumber", "1.0"))),
        new Pair<>(new MessageQueue(topic, BROKER_NAME, 1), new Message(topic, "TagA", jb("2049", "f", "y", "10", "20.0", "notanumber"))),
        new Pair<>(new MessageQueue(topic, BROKER_NAME, 1), new Message(topic, "TagA", jb("2012", "g", "y", "10", "20.0", "1.0"))),
        new Pair<>(new MessageQueue(topic, BROKER_NAME, 1), new Message(topic, "TagA", jb("2011", "h", "y", "10", "20.0", "1.0")))
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

  private List<OrderedPartitionableRecord<String, Long, RocketMQRecordEntity>> createOrderedPartitionableRecords()
  {
    Map<String, Long> partitionToOffset = new HashMap<>();
    return records.stream().map(r -> {
      long offset = 0;
      if (partitionToOffset.containsKey(PartitionUtil.genPartition(r.lhs))) {
        offset = partitionToOffset.get(PartitionUtil.genPartition(r.lhs));
        partitionToOffset.put(PartitionUtil.genPartition(r.lhs), offset + 1);
      } else {
        partitionToOffset.put(PartitionUtil.genPartition(r.lhs), 1L);
      }
      return new OrderedPartitionableRecord<>(
          topic,
          PartitionUtil.genPartition(r.lhs),
          offset,
          r.rhs == null ? null : Collections.singletonList(new RocketMQRecordEntity(
              r.rhs.getBody()
          ))
      );
    }).collect(Collectors.toList());
  }

  @BeforeClass
  public static void setupClass() throws Exception
  {
    rocketmqServer = new TestBroker(
        null,
        ImmutableMap.of("default.topic.queue.nums", 2)
    );
    rocketmqServer.start();
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
    rocketmqServer.close();
    rocketmqServer = null;
  }

  @Test
  public void testSupplierSetup() throws InterruptedException, RemotingException, MQClientException, MQBrokerException
  {

    // Insert data
    insertData();

    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        StreamPartition.of(topic, PartitionUtil.genPartition(BROKER_NAME, 0)),
        StreamPartition.of(topic, PartitionUtil.genPartition(BROKER_NAME, 1))
    );

    RocketMQRecordSupplier recordSupplier = new RocketMQRecordSupplier(
        rocketmqServer.consumerProperties());

    Assert.assertTrue(recordSupplier.getAssignment().isEmpty());

    recordSupplier.assign(partitions);

    Assert.assertEquals(partitions, recordSupplier.getAssignment());
    Assert.assertEquals(ImmutableSet.of(PartitionUtil.genPartition(BROKER_NAME, 0), PartitionUtil.genPartition(BROKER_NAME, 1)), recordSupplier.getPartitionIds(topic));

    recordSupplier.close();
  }

  @Test
  public void testPoll() throws InterruptedException, RemotingException, MQClientException, MQBrokerException
  {

    // Insert data
    insertData();

    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        StreamPartition.of(topic, PartitionUtil.genPartition(BROKER_NAME, 0)),
        StreamPartition.of(topic, PartitionUtil.genPartition(BROKER_NAME, 1))
    );

    RocketMQRecordSupplier recordSupplier = new RocketMQRecordSupplier(
        rocketmqServer.consumerProperties());

    recordSupplier.assign(partitions);
    recordSupplier.seekToEarliest(partitions);

    List<OrderedPartitionableRecord<String, Long, RocketMQRecordEntity>> initialRecords = new ArrayList<>(createOrderedPartitionableRecords());

    List<OrderedPartitionableRecord<String, Long, RocketMQRecordEntity>> polledRecords = recordSupplier.poll(poll_timeout_millis);
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
  public void testPollAfterMoreDataAdded() throws InterruptedException, MQClientException, RemotingException, MQBrokerException
  {

    // Insert data
    TestProducer.produceAndConfirm(rocketmqServer, records.subList(0, 13));

    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        StreamPartition.of(topic, PartitionUtil.genPartition(BROKER_NAME, 0)),
        StreamPartition.of(topic, PartitionUtil.genPartition(BROKER_NAME, 1))
    );

    RocketMQRecordSupplier recordSupplier = new RocketMQRecordSupplier(
        rocketmqServer.consumerProperties());

    recordSupplier.assign(partitions);
    recordSupplier.seekToEarliest(partitions);

    List<OrderedPartitionableRecord<String, Long, RocketMQRecordEntity>> polledRecords = recordSupplier.poll(poll_timeout_millis);
    for (int i = 0; polledRecords.size() != 13 && i < pollRetry; i++) {
      polledRecords.addAll(recordSupplier.poll(poll_timeout_millis));
      Thread.sleep(200);
    }

    // Insert data
    TestProducer.produceAndConfirm(rocketmqServer, records.subList(13, 14));


    for (int i = 0; polledRecords.size() != records.size() && i < pollRetry; i++) {
      polledRecords.addAll(recordSupplier.poll(poll_timeout_millis));
      Thread.sleep(200);
    }

    List<OrderedPartitionableRecord<String, Long, RocketMQRecordEntity>> initialRecords = createOrderedPartitionableRecords();

    Assert.assertEquals(records.size(), polledRecords.size());
    Assert.assertEquals(partitions, recordSupplier.getAssignment());

    final int initialRecordsPartition0Size = initialRecords.stream()
        .filter(r -> r.getPartitionId().equals(PartitionUtil.genPartition(BROKER_NAME, 0)))
        .collect(Collectors.toSet())
        .size();
    final int initialRecordsPartition1Size = initialRecords.stream()
        .filter(r -> r.getPartitionId().equals(PartitionUtil.genPartition(BROKER_NAME, 1)))
        .collect(Collectors.toSet())
        .size();

    final int polledRecordsPartition0Size = polledRecords.stream()
        .filter(r -> r.getPartitionId().equals(PartitionUtil.genPartition(BROKER_NAME, 0)))
        .collect(Collectors.toSet())
        .size();
    final int polledRecordsPartition1Size = polledRecords.stream()
        .filter(r -> r.getPartitionId().equals(PartitionUtil.genPartition(BROKER_NAME, 1)))
        .collect(Collectors.toSet())
        .size();

    Assert.assertEquals(initialRecordsPartition0Size, polledRecordsPartition0Size);
    Assert.assertEquals(initialRecordsPartition1Size, polledRecordsPartition1Size);

    recordSupplier.close();
  }

  @Test
  public void testSeek() throws InterruptedException, RemotingException, MQClientException, MQBrokerException
  {
    // Insert data
    insertData();

    StreamPartition<String> partition0 = StreamPartition.of(topic, PartitionUtil.genPartition(BROKER_NAME, 0));
    StreamPartition<String> partition1 = StreamPartition.of(topic, PartitionUtil.genPartition(BROKER_NAME, 1));

    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        StreamPartition.of(topic, PartitionUtil.genPartition(BROKER_NAME, 0)),
        StreamPartition.of(topic, PartitionUtil.genPartition(BROKER_NAME, 1))
    );

    RocketMQRecordSupplier recordSupplier = new RocketMQRecordSupplier(
        rocketmqServer.consumerProperties());

    recordSupplier.assign(partitions);
    recordSupplier.seekToEarliest(partitions);

    Assert.assertEquals(0L, (long) recordSupplier.getEarliestSequenceNumber(partition0));
    Assert.assertEquals(0L, (long) recordSupplier.getEarliestSequenceNumber(partition1));

    recordSupplier.seek(partition0, 2L);
    recordSupplier.seek(partition1, 2L);

    List<OrderedPartitionableRecord<String, Long, RocketMQRecordEntity>> initialRecords = createOrderedPartitionableRecords();

    List<OrderedPartitionableRecord<String, Long, RocketMQRecordEntity>> polledRecords = recordSupplier.poll(poll_timeout_millis);
    for (int i = 0; polledRecords.size() != 10 && i < pollRetry; i++) {
      List<OrderedPartitionableRecord<String, Long, RocketMQRecordEntity>> list = recordSupplier.poll(poll_timeout_millis);
      if (list.size() > 0) {
        polledRecords.addAll(list);
      }
      Thread.sleep(3000);
    }


    Assert.assertEquals(10, polledRecords.size());
    Assert.assertTrue(initialRecords.containsAll(polledRecords));


    recordSupplier.close();

  }

  @Test
  public void testSeekToLatest() throws InterruptedException, RemotingException, MQClientException, MQBrokerException
  {
    // Insert data
    insertData();

    StreamPartition<String> partition0 = StreamPartition.of(topic, PartitionUtil.genPartition(BROKER_NAME, 0));
    StreamPartition<String> partition1 = StreamPartition.of(topic, PartitionUtil.genPartition(BROKER_NAME, 1));

    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        StreamPartition.of(topic, PartitionUtil.genPartition(BROKER_NAME, 0)),
        StreamPartition.of(topic, PartitionUtil.genPartition(BROKER_NAME, 1))
    );

    RocketMQRecordSupplier recordSupplier = new RocketMQRecordSupplier(
        rocketmqServer.consumerProperties());

    recordSupplier.assign(partitions);
    recordSupplier.seekToEarliest(partitions);

    Assert.assertEquals(0L, (long) recordSupplier.getEarliestSequenceNumber(partition0));
    Assert.assertEquals(0L, (long) recordSupplier.getEarliestSequenceNumber(partition1));

    recordSupplier.seekToLatest(partitions);
    List<OrderedPartitionableRecord<String, Long, RocketMQRecordEntity>> polledRecords = recordSupplier.poll(poll_timeout_millis);
    Assert.assertEquals(Collections.emptyList(), polledRecords);
    recordSupplier.close();
  }

  @Test
  public void getLatestSequenceNumberWhenPartitionIsEmptyAndUseEarliestOffsetShouldReturnsValidNonNull()
  {
    RocketMQRecordSupplier recordSupplier = new RocketMQRecordSupplier(
        rocketmqServer.consumerProperties());
    StreamPartition<String> streamPartition = StreamPartition.of(topic, PartitionUtil.genPartition(BROKER_NAME, 0));
    Set<StreamPartition<String>> partitions = ImmutableSet.of(streamPartition);
    recordSupplier.assign(partitions);
    recordSupplier.seekToEarliest(partitions);
    Assert.assertEquals(new Long(0), recordSupplier.getLatestSequenceNumber(streamPartition));
  }

  @Test
  public void getEarliestSequenceNumberWhenPartitionIsEmptyAndUseEarliestOffsetShouldReturnsValidNonNull()
  {
    RocketMQRecordSupplier recordSupplier = new RocketMQRecordSupplier(
        rocketmqServer.consumerProperties());
    StreamPartition<String> streamPartition = StreamPartition.of(topic, PartitionUtil.genPartition(BROKER_NAME, 0));
    Set<StreamPartition<String>> partitions = ImmutableSet.of(streamPartition);
    recordSupplier.assign(partitions);
    recordSupplier.seekToEarliest(partitions);
    Assert.assertEquals(new Long(0), recordSupplier.getEarliestSequenceNumber(streamPartition));
  }

  @Test
  public void getLatestSequenceNumberWhenPartitionIsEmptyAndUseLatestOffsetShouldReturnsValidNonNull()
  {
    RocketMQRecordSupplier recordSupplier = new RocketMQRecordSupplier(
        rocketmqServer.consumerProperties());
    StreamPartition<String> streamPartition = StreamPartition.of(topic, PartitionUtil.genPartition(BROKER_NAME, 0));
    Set<StreamPartition<String>> partitions = ImmutableSet.of(streamPartition);
    recordSupplier.assign(partitions);
    recordSupplier.seekToLatest(partitions);
    Assert.assertEquals(new Long(0), recordSupplier.getLatestSequenceNumber(streamPartition));
  }

  @Test
  public void getEarliestSequenceNumberWhenPartitionIsEmptyAndUseLatestOffsetShouldReturnsValidNonNull()
  {
    RocketMQRecordSupplier recordSupplier = new RocketMQRecordSupplier(
        rocketmqServer.consumerProperties());
    StreamPartition<String> streamPartition = StreamPartition.of(topic, PartitionUtil.genPartition(BROKER_NAME, 0));
    Set<StreamPartition<String>> partitions = ImmutableSet.of(streamPartition);
    recordSupplier.assign(partitions);
    recordSupplier.seekToLatest(partitions);
    Assert.assertEquals(new Long(0), recordSupplier.getEarliestSequenceNumber(streamPartition));
  }

  private void insertData() throws MQClientException, RemotingException, InterruptedException, MQBrokerException
  {
    TestProducer.produceAndConfirm(rocketmqServer, records);
  }
}
