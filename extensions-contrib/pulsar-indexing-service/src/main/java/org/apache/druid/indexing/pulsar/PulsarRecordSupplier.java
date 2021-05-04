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
import org.apache.druid.indexing.pulsar.supervisor.PulsarSupervisorIOConfig;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.common.StreamException;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.metadata.DynamicConfigProvider;
import org.apache.druid.metadata.PasswordProvider;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.MultiTopicsConsumerImpl;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public class PulsarRecordSupplier implements RecordSupplier<String, String, PulsarRecordEntity>
{
  private static final EmittingLogger log = new EmittingLogger(PulsarRecordSupplier.class);
  private MultiTopicsConsumerImpl consumers;
  private boolean closed;
  private final PulsarClient client;

  public PulsarRecordSupplier(
      Map<String, Object> consumerProperties,
      ObjectMapper sortingMapper
  )
  {
    this.client = getPulsarClient(sortingMapper, consumerProperties);
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
    List<String> topicPartions = new ArrayList<>();
    for (StreamPartition<String> integerStreamPartition : streamPartitions) {
      topicPartions.add(integerStreamPartition.getPartitionId());
    }
    try {
      consumers = (MultiTopicsConsumerImpl) client.newConsumer().topics(topicPartions).subscribe();
    }
    catch (PulsarClientException e) {
      log.error(e.getMessage());
    }
  }

  @Override
  public void seek(StreamPartition<String> partition, String sequenceNumber)
  {
    String[] sequenceNumbers = sequenceNumber.split(":");
    List<ConsumerImpl> consumersList = consumers.getConsumers();
    for (Consumer consumer : consumersList) {
      try {
        consumer.seek(new MessageIdImpl(Integer.getInteger(sequenceNumbers[0]), Integer.getInteger(sequenceNumbers[1]), Integer.getInteger(sequenceNumbers[2])));
      }
      catch (PulsarClientException e) {
        log.error(e.getMessage());
      }
    }
  }

  @Override
  public void seekToEarliest(Set<StreamPartition<String>> partitions)
  {
    try {
      consumers.seek(MessageId.earliest);
    }
    catch (PulsarClientException e) {
      log.error(e.getMessage());
    }
  }

  @Override
  public void seekToLatest(Set<StreamPartition<String>> partitions)
  {
    try {
      consumers.seek(MessageId.latest);
    }
    catch (PulsarClientException e) {
      log.error(e.getMessage());
    }
  }

  @Override
  public Set<StreamPartition<String>> getAssignment()
  {
    Set<StreamPartition<String>> streamPartitions = new HashSet<>();
    List<String> topicPartitions = consumers.getTopics();
    for (String topicPartition : topicPartitions) {
      //TODO: split regex
      String topic = topicPartition.split("partition-")[0];
      streamPartitions.add(new StreamPartition<>(topic, topicPartition));
    }
    return streamPartitions;
  }

  @Nonnull
  @Override
  public List<OrderedPartitionableRecord<String, String, PulsarRecordEntity>> poll(long timeout)
  {
    List<OrderedPartitionableRecord<String, String, PulsarRecordEntity>> polledRecords = new ArrayList<>();
    Iterator messageIterator = null;
    try {
      messageIterator = consumers.batchReceive().iterator();
    }
    catch (PulsarClientException e) {
      log.error(e.getMessage());
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
    return polledRecords;
  }

  @Override
  public String getLatestSequenceNumber(StreamPartition<String> partition)
  {
    String pos = null;
    List<ConsumerImpl> consumersList = consumers.getConsumers();
    for (Consumer consumer : consumersList) {
      if (consumer.getTopic().equals(partition.getStream() + "partition-" + partition.getPartitionId())) {
        try {
          pos = consumer.getLastMessageId().toString();
          break;
        }
        catch (PulsarClientException e) {
          log.error(e.getMessage());
        }
      }
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
    Set<Integer> partitions = new HashSet<>();
    List<String> topicPartitions = null;
    try {
      topicPartitions = client.getPartitionsForTopic(stream).get();
    }
    catch (InterruptedException e) {
      log.error(e.getMessage());
    }
    catch (ExecutionException e) {
      log.error(e.getMessage());
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
    try {
      consumers.close();
    }
    catch (PulsarClientException e) {
      log.error(e.getMessage());
    }
  }

  public static void addConsumerPropertiesFromConfig(
      Properties properties,
      ObjectMapper configMapper,
      Map<String, Object> consumerProperties
  )
  {
    // Extract passwords before SSL connection to Pulsar
    for (Map.Entry<String, Object> entry : consumerProperties.entrySet()) {
      String propertyKey = entry.getKey();

      if (!PulsarSupervisorIOConfig.DRUID_DYNAMIC_CONFIG_PROVIDER_KEY.equals(propertyKey)) {
        if (propertyKey.equals(PulsarSupervisorIOConfig.TRUST_STORE_PASSWORD_KEY)
            || propertyKey.equals(PulsarSupervisorIOConfig.KEY_STORE_PASSWORD_KEY)
            || propertyKey.equals(PulsarSupervisorIOConfig.KEY_PASSWORD_KEY)) {
          PasswordProvider configPasswordProvider = configMapper.convertValue(
              entry.getValue(),
              PasswordProvider.class
          );
          properties.setProperty(propertyKey, configPasswordProvider.getPassword());
        } else {
          properties.setProperty(propertyKey, String.valueOf(entry.getValue()));
        }
      }
    }

    // Additional DynamicConfigProvider based extensible support for all consumer properties
    Object dynamicConfigProviderJson = consumerProperties.get(PulsarSupervisorIOConfig.DRUID_DYNAMIC_CONFIG_PROVIDER_KEY);
    if (dynamicConfigProviderJson != null) {
      DynamicConfigProvider dynamicConfigProvider = configMapper.convertValue(dynamicConfigProviderJson, DynamicConfigProvider.class);
      Map<String, String> dynamicConfig = dynamicConfigProvider.getConfig();

      for (Map.Entry<String, String> e : dynamicConfig.entrySet()) {
        properties.setProperty(e.getKey(), e.getValue());
      }
    }
  }

  private static PulsarClient getPulsarClient(ObjectMapper sortingMapper, Map<String, Object> consumerProperties)
  {
    final Map<String, Object> consumerConfigs = PulsarConsumerConfigs.getConsumerProperties();
    final Properties props = new Properties();
    addConsumerPropertiesFromConfig(props, sortingMapper, consumerProperties);
    props.putAll(consumerConfigs);

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
        log.error(e.getMessage());
      }
      return client;
    }
    finally {
      Thread.currentThread().setContextClassLoader(currCtxCl);
    }
  }

  private static <T> T wrapExceptions(Callable<T> callable)
  {
    try {
      return callable.call();
    }
    catch (Exception e) {
      throw new StreamException(e);
    }
  }

  private static void wrapExceptions(Runnable runnable)
  {
    wrapExceptions(() -> {
      runnable.run();
      return null;
    });
  }
}
