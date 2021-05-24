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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.rocketmq.PartitionUtil;
import org.apache.druid.data.input.rocketmq.RocketMQRecordEntity;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;


import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RocketMQRecordSupplier implements RecordSupplier<String, Long, RocketMQRecordEntity>
{
  private static final EmittingLogger log = new EmittingLogger(RocketMQRecordSupplier.class);
  private final DefaultLitePullConsumer consumer;
  private boolean closed;
  private Set<StreamPartition<String>> streamPartitions;

  public RocketMQRecordSupplier(
      Map<String, Object> consumerProperties,
      ObjectMapper sortingMapper
  )
  {
    this(getRocketMQConsumer(sortingMapper, consumerProperties));
  }

  @VisibleForTesting
  public RocketMQRecordSupplier(
      DefaultLitePullConsumer consumer
  )
  {
    this.consumer = consumer;
  }

  @Override
  public void assign(Set<StreamPartition<String>> streamPartitions)
  {
    this.streamPartitions = streamPartitions;
    Set<MessageQueue> messageQueues = new HashSet<>();
    for (StreamPartition<String> streamPartition : streamPartitions) {
      MessageQueue mq = PartitionUtil.genMessageQueue(streamPartition.getStream(), streamPartition.getPartitionId());
      messageQueues.add(mq);
    }

    if (!messageQueues.isEmpty()) {
      consumer.assign(messageQueues);
    }

    try {
      if (!consumer.isRunning()) {
        consumer.start();
      }
    }
    catch (MQClientException e) {
      log.error(e.getErrorMessage());
    }
  }

  @Override
  public void seek(StreamPartition<String> partition, Long sequenceNumber)
  {
    MessageQueue mq = PartitionUtil.genMessageQueue(partition.getStream(), partition.getPartitionId());
    try {
      consumer.seek(mq, sequenceNumber);
    }
    catch (MQClientException e) {
      log.error(e.getErrorMessage());
    }
  }

  @Override
  public void seekToEarliest(Set<StreamPartition<String>> partitions)
  {
    for (StreamPartition<String> partition : partitions) {
      MessageQueue mq = PartitionUtil.genMessageQueue(partition.getStream(), partition.getPartitionId());
      try {
        consumer.seekToBegin(mq);
      }
      catch (MQClientException e) {
        log.error(e.getErrorMessage());
      }
    }
  }

  @Override
  public void seekToLatest(Set<StreamPartition<String>> partitions)
  {
    for (StreamPartition<String> partition : partitions) {
      MessageQueue mq = PartitionUtil.genMessageQueue(partition.getStream(), partition.getPartitionId());
      try {
        consumer.seekToEnd(mq);
      }
      catch (MQClientException e) {
        log.error(e.getErrorMessage());
      }
    }
  }

  @Override
  public Set<StreamPartition<String>> getAssignment()
  {
    if (this.streamPartitions == null) {
      return new HashSet<StreamPartition<String>>();
    }
    return this.streamPartitions;
  }

  @Nonnull
  @Override
  public List<OrderedPartitionableRecord<String, Long, RocketMQRecordEntity>> poll(long timeout)
  {
    List<OrderedPartitionableRecord<String, Long, RocketMQRecordEntity>> polledRecords = new ArrayList<>();
    for (MessageExt messageExt : consumer.poll(timeout)) {

      polledRecords.add(new OrderedPartitionableRecord<>(
          messageExt.getTopic(),
          PartitionUtil.genPartition(messageExt.getBrokerName(), messageExt.getQueueId()),
          messageExt.getQueueOffset(),
          messageExt.getBody() == null ? null : ImmutableList.of(new RocketMQRecordEntity(messageExt.getBody()))
      ));
    }
    return polledRecords;
  }

  @Override
  public Long getLatestSequenceNumber(StreamPartition<String> partition)
  {
    Long currPos = getPosition(partition);
    seekToLatest(Collections.singleton(partition));
    Long nextPos = getPosition(partition);
    seek(partition, currPos);
    return nextPos;
  }

  @Override
  public Long getEarliestSequenceNumber(StreamPartition<String> partition)
  {
    Long currPos = getPosition(partition);
    seekToEarliest(Collections.singleton(partition));
    Long nextPos = getPosition(partition);
    seek(partition, currPos);
    return nextPos;
  }

  @Override
  public Long getPosition(StreamPartition<String> partition)
  {
    MessageQueue mq = PartitionUtil.genMessageQueue(partition.getStream(), partition.getPartitionId());
    return consumer.getOffsetStore().readOffset(mq, ReadOffsetType.MEMORY_FIRST_THEN_STORE);
  }

  @Override
  public Set<String> getPartitionIds(String stream)
  {
    HashSet<String> partitionIds = new HashSet<>();
    Collection<MessageQueue> messageQueues = null;
    try {
      if (!consumer.isRunning()) {
        consumer.start();
      }
      messageQueues = consumer.fetchMessageQueues(stream);
    }
    catch (MQClientException e) {
      log.error(e.getErrorMessage());
    }
    for (MessageQueue messageQueue : messageQueues) {
      partitionIds.add(PartitionUtil.genPartition(messageQueue));
    }
    return partitionIds;
  }

  @Override
  public void close()
  {
    if (closed) {
      return;
    }
    closed = true;
    consumer.shutdown();
  }

  private static DefaultLitePullConsumer getRocketMQConsumer(ObjectMapper sortingMapper, Map<String, Object> consumerProperties)
  {
    final Map<String, Object> consumerConfigs = RocketMQConsumerConfigs.getConsumerProperties();
    DefaultLitePullConsumer consumer = new DefaultLitePullConsumer(consumerConfigs.get("consumer.group").toString());
    consumer.setNamesrvAddr(consumerProperties.get("nameserver.url").toString());
    return consumer;
  }
}
