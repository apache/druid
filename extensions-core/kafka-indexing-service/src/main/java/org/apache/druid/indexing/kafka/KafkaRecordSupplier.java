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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorIOConfig;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class KafkaRecordSupplier implements RecordSupplier<Integer, Long>
{
  private static final EmittingLogger log = new EmittingLogger(KafkaRecordSupplier.class);
  private static final Random RANDOM = ThreadLocalRandom.current();

  private final KafkaConsumer<byte[], byte[]> consumer;
  private final KafkaSupervisorIOConfig ioConfig;
  private boolean closed;
  private final BlockingQueue<OrderedPartitionableRecord<Integer, Long>> records;


  public KafkaRecordSupplier(
      KafkaSupervisorIOConfig ioConfig
  )
  {
    this.ioConfig = ioConfig;
    this.consumer = getKafkaConsumer();
    this.closed = false;
    this.records = new LinkedBlockingQueue<>();
  }

  @Override
  public void assign(Set<StreamPartition<Integer>> streamPartitions)
  {
    consumer.assign(streamPartitions
                        .stream()
                        .map(x -> new TopicPartition(x.getStream(), x.getPartitionId()))
                        .collect(Collectors.toSet()));
  }

  @Override
  public void seek(StreamPartition<Integer> partition, Long sequenceNumber)
  {
    consumer.seek(new TopicPartition(partition.getStream(), partition.getPartitionId()), sequenceNumber);
  }

  @Override
  public void seekAfter(StreamPartition<Integer> partition, Long sequenceNumber)
  {
    seek(partition, sequenceNumber + 1);
  }

  @Override
  public void seekToEarliest(Set<StreamPartition<Integer>> partitions)
  {
    consumer.seekToBeginning(partitions
                                 .stream()
                                 .map(e -> new TopicPartition(e.getStream(), e.getPartitionId()))
                                 .collect(Collectors.toList()));
  }

  @Override
  public void seekToLatest(Set<StreamPartition<Integer>> partitions)
  {
    consumer.seekToEnd(partitions
                           .stream()
                           .map(e -> new TopicPartition(e.getStream(), e.getPartitionId()))
                           .collect(Collectors.toList()));
  }

  @Override
  public Set<StreamPartition<Integer>> getAssignment()
  {
    Set<TopicPartition> topicPartitions = consumer.assignment();
    return topicPartitions
        .stream()
        .map((TopicPartition e) -> new StreamPartition<>(e.topic(), e.partition()))
        .collect(Collectors.toSet());
  }

  @Override
  public OrderedPartitionableRecord<Integer, Long> poll(long timeout)
  {
    if (records.isEmpty()) {
      ConsumerRecords<byte[], byte[]> polledRecords = consumer.poll(timeout);
      for (ConsumerRecord<byte[], byte[]> record : polledRecords) {
        records.offer(new OrderedPartitionableRecord<>(
            record.topic(),
            record.partition(),
            record.offset(),
            ImmutableList.of(record.value())
        ));
      }
    }

    try {
      return records.poll(timeout, TimeUnit.MILLISECONDS);
    }
    catch (InterruptedException e) {
      log.warn(e, "InterruptedException");
      return null;
    }
  }

  @Override
  public Long getLatestSequenceNumber(StreamPartition<Integer> partition)
  {
    seekToLatest(Collections.singleton(partition));
    return consumer.position(new TopicPartition(partition.getStream(), partition.getPartitionId()));
  }

  @Override
  public Long getEarliestSequenceNumber(StreamPartition<Integer> partition)
  {
    seekToEarliest(Collections.singleton(partition));
    return consumer.position(new TopicPartition(partition.getStream(), partition.getPartitionId()));
  }

  @Override
  public Long position(StreamPartition<Integer> partition)
  {
    return consumer.position(new TopicPartition(partition.getStream(), partition.getPartitionId()));
  }

  @Override
  public Set<Integer> getPartitionIds(String stream)
  {
    final Map<String, List<PartitionInfo>> topics = consumer.listTopics();
    if (!topics.containsKey(stream)) {
      throw new ISE("Topic [%s] is not found in KafkaConsumer's list of topics", stream);
    }
    return topics == null
           ? ImmutableSet.of()
           : topics.get(stream).stream().map(PartitionInfo::partition).collect(Collectors.toSet());
  }

  @Override
  public void close()
  {
    if (closed) {
      return;
    }
    closed = true;
    consumer.close();
  }

  private KafkaConsumer<byte[], byte[]> getKafkaConsumer()
  {
    final Properties props = new Properties();

    props.setProperty("metadata.max.age.ms", "10000");
    props.setProperty("group.id", StringUtils.format("kafka-supervisor-%s", getRandomId()));

    props.putAll(ioConfig.getConsumerProperties());

    props.setProperty("enable.auto.commit", "false");

    ClassLoader currCtxCl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
      return new KafkaConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }
    finally {
      Thread.currentThread().setContextClassLoader(currCtxCl);
    }
  }

  private static String getRandomId()
  {
    final StringBuilder suffix = new StringBuilder(8);
    for (int i = 0; i < Integer.BYTES * 2; ++i) {
      suffix.append((char) ('a' + ((RANDOM.nextInt() >>> (i * 4)) & 0x0F)));
    }
    return suffix.toString();
  }
}
