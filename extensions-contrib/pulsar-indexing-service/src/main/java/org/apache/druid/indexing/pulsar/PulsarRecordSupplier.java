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

package org.apache.druid.indexing.pulsar;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.pulsar.PulsarRecordEntity;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.MessageIdImpl;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class PulsarRecordSupplier implements RecordSupplier<String, String, PulsarRecordEntity>
{
  private static final EmittingLogger log = new EmittingLogger(PulsarRecordSupplier.class);
  private boolean closed;
  private final PulsarClient client;
  private final ConcurrentHashMap<String, Consumer> consumerHashMap = new ConcurrentHashMap<>();

  public PulsarRecordSupplier(
      Map<String, Object> consumerProperties,
      ObjectMapper sortingMapper
  )
  {
    this.client = getPulsarClient(consumerProperties);
  }

  @VisibleForTesting
  public PulsarRecordSupplier(
      PulsarClient client
  )
  {
    this.client = client;
  }

  @Override
  public void assign(Set<StreamPartition<String>> streamPartitions)
  {
    final Map<String, Object> consumerConfigs = PulsarConsumerConfigs.getConsumerProperties();
    for (StreamPartition<String> streamPartition : streamPartitions) {
      try {
        String topicPartition = streamPartition.getPartitionId();
        Consumer consumer = client.newConsumer()
            .topic(topicPartition)
            .subscriptionName((String) consumerConfigs.get("subscription.name"))
            .subscriptionType(SubscriptionType.Exclusive)
            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
            .negativeAckRedeliveryDelay(60, TimeUnit.SECONDS)
            .subscribe();
        consumerHashMap.put(topicPartition, consumer);
      }
      catch (PulsarClientException e) {
        log.error("Could not assign partitions." + e.getMessage());
      }
    }
  }

  @Override
  public void seek(StreamPartition<String> partition, String sequenceNumber)
  {
    String[] sequenceNumbers = sequenceNumber.split(":");
    try {
      Consumer consumer = consumerHashMap.get(partition.getPartitionId());
      consumer.getLastMessageId();
      MessageIdImpl messageId = new MessageIdImpl(Long.parseLong(sequenceNumbers[0]), Long.parseLong(sequenceNumbers[1]), Integer.parseInt(sequenceNumbers[2]));
      consumer.seek(messageId);
    }
    catch (PulsarClientException e) {
      log.error("Could not seek pulsar offset." + e.getMessage());
    }
  }

  @Override
  public void seekToEarliest(Set<StreamPartition<String>> partitions)
  {
    for (StreamPartition<String> partition : partitions) {
      try {
        consumerHashMap.get(partition.getPartitionId()).seek(MessageId.earliest);
      }
      catch (PulsarClientException e) {
        log.error("Could not seek earliest offset." + e.getMessage());
      }
    }
  }

  @Override
  public void seekToLatest(Set<StreamPartition<String>> partitions)
  {
    for (StreamPartition<String> partition : partitions) {
      try {
        consumerHashMap.get(partition.getPartitionId()).seek(MessageId.latest);
      }
      catch (PulsarClientException e) {
        log.error("Could not seek to latest offset." + e.getMessage());
      }
    }
  }

  @Override
  public Set<StreamPartition<String>> getAssignment()
  {
    Set<StreamPartition<String>> streamPartitions = new HashSet<>();
    for (String topicPartition : consumerHashMap.keySet()) {
      String topic = topicPartition.split("-partition-")[0];
      streamPartitions.add(new StreamPartition<>(topic, topicPartition));
    }
    return streamPartitions;
  }

  @Nonnull
  @Override
  public List<OrderedPartitionableRecord<String, String, PulsarRecordEntity>> poll(long timeout)
  {
    List<OrderedPartitionableRecord<String, String, PulsarRecordEntity>> polledRecords = new ArrayList<>();
    for (String topicPartition : consumerHashMap.keySet()) {
      Consumer consumer = consumerHashMap.get(topicPartition);
      Iterator messageIterator = null;
      try {
        messageIterator = consumer.batchReceive().iterator();
      }
      catch (PulsarClientException e) {
        log.error("Could not poll mssafes." + e.getMessage());
      }
      while (messageIterator.hasNext()) {
        Message message = (Message) messageIterator.next();
        polledRecords.add(new OrderedPartitionableRecord<>(
            message.getTopicName().split("partition-")[0],
            message.getTopicName(),
            message.getMessageId().toString(),
            message.getValue() == null ? null : ImmutableList.of(new PulsarRecordEntity(message))
        ));
      }
    }
    return polledRecords;
  }

  @Override
  public String getLatestSequenceNumber(StreamPartition<String> partition)
  {
    String pos = null;
    try {
      pos = consumerHashMap.get(partition.getPartitionId()).getLastMessageId().toString();
    }
    catch (PulsarClientException e) {
      log.error("Could not get latest message ID. " + e.getMessage());
    }
    return pos;
  }

  @Override
  public String getEarliestSequenceNumber(StreamPartition<String> partition)
  {
    return MessageId.earliest.toString();
  }

  @Nullable
  @Override
  public String getPosition(StreamPartition<String> partition)
  {
    throw new UnsupportedOperationException("getPosition() is not supported in Pulsar");
  }

  @Override
  public Set<String> getPartitionIds(String stream)
  {
    List<String> topicPartitions = null;
    try {
      topicPartitions = client.getPartitionsForTopic(stream).get();
    }
    catch (InterruptedException e) {
      log.error("Could not get partitions. " + e.getMessage());
    }
    catch (ExecutionException e) {
      log.error("Could not get partitions. " + e.getMessage());
    }
    return new HashSet<String>(topicPartitions);
  }

  @Override
  public void close()
  {
    if (closed) {
      return;
    }
    closed = true;
    for (String topicPartition : consumerHashMap.keySet()) {
      try {
        consumerHashMap.get(topicPartition).close();
      }
      catch (PulsarClientException e) {
        log.error("Could not get close consumer. " + e.getMessage());
      }
    }
  }

  private static PulsarClient getPulsarClient(Map<String, Object> consumerProperties)
  {
    ClassLoader currCtxCl = Thread.currentThread().getContextClassLoader();
    PulsarClient client = null;
    try {
      Thread.currentThread().setContextClassLoader(PulsarRecordSupplier.class.getClassLoader());
      try {
        client = PulsarClient.builder()
            .serviceUrl((String) consumerProperties.get("pulsar.url"))
            .build();
      }
      catch (PulsarClientException e) {
        log.error("Could not create pulsar client. " + e.getMessage());
      }
      return client;
    }
    finally {
      Thread.currentThread().setContextClassLoader(currCtxCl);
    }
  }

}
