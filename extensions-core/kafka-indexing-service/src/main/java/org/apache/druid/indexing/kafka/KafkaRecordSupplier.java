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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.data.input.kafka.KafkaTopicPartition;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorIOConfig;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.indexing.seekablestream.common.OrderedSequenceNumber;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.common.StreamException;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.indexing.seekablestream.extension.KafkaConfigOverrides;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.metrics.Monitor;
import org.apache.druid.metadata.DynamicConfigProvider;
import org.apache.druid.metadata.PasswordProvider;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;

import javax.annotation.Nonnull;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class KafkaRecordSupplier implements RecordSupplier<KafkaTopicPartition, Long, KafkaRecordEntity>
{
  private final KafkaConsumer<byte[], byte[]> consumer;
  private final KafkaConsumerMonitor monitor;
  private boolean closed;

  private final boolean multiTopic;

  /**
   * Store the stream information when partitions get assigned. This is required because the consumer does not
   * know about the parent stream which could be a list of topics.
   */
  private String stream;

  public KafkaRecordSupplier(
      Map<String, Object> consumerProperties,
      ObjectMapper sortingMapper,
      KafkaConfigOverrides configOverrides,
      boolean multiTopic
  )
  {
    this(getKafkaConsumer(sortingMapper, consumerProperties, configOverrides), multiTopic);
  }

  @VisibleForTesting
  public KafkaRecordSupplier(
      KafkaConsumer<byte[], byte[]> consumer,
      boolean multiTopic
  )
  {
    this.consumer = consumer;
    this.multiTopic = multiTopic;
    this.monitor = new KafkaConsumerMonitor(consumer);
  }

  @Override
  public void assign(Set<StreamPartition<KafkaTopicPartition>> streamPartitions)
  {
    if (streamPartitions.isEmpty()) {
      wrapExceptions(() -> consumer.assign(Collections.emptyList()));
      return;
    }

    Set<String> streams = streamPartitions.stream().map(StreamPartition::getStream).collect(Collectors.toSet());
    if (streams.size() > 1) {
      throw InvalidInput.exception("[%s] streams found. Only one stream is supported.", streams);
    }
    this.stream = streams.iterator().next();
    wrapExceptions(() -> consumer.assign(streamPartitions
                                             .stream()
                                             .map(x -> x.getPartitionId().asTopicPartition(x.getStream()))
                                             .collect(Collectors.toSet())));
  }

  @Override
  public void seek(StreamPartition<KafkaTopicPartition> partition, Long sequenceNumber)
  {
    wrapExceptions(() -> consumer.seek(
        partition.getPartitionId().asTopicPartition(partition.getStream()),
        sequenceNumber
    ));
  }

  @Override
  public void seekToEarliest(Set<StreamPartition<KafkaTopicPartition>> partitions)
  {
    wrapExceptions(() -> consumer.seekToBeginning(partitions
                                                      .stream()
                                                      .map(e -> e.getPartitionId().asTopicPartition(e.getStream()))
                                                      .collect(Collectors.toList())));
  }

  @Override
  public void seekToLatest(Set<StreamPartition<KafkaTopicPartition>> partitions)
  {
    wrapExceptions(() -> consumer.seekToEnd(partitions
                                                .stream()
                                                .map(e -> e.getPartitionId().asTopicPartition(e.getStream()))
                                                .collect(Collectors.toList())));
  }

  @Override
  public Set<StreamPartition<KafkaTopicPartition>> getAssignment()
  {
    return wrapExceptions(() -> consumer.assignment()
                                        .stream()
                                        .map(e -> new StreamPartition<>(
                                            stream,
                                            new KafkaTopicPartition(multiTopic, e.topic(),
                                                                    e.partition()
                                            )
                                        ))
                                        .collect(Collectors.toSet()));
  }

  @Nonnull
  @Override
  public List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> poll(long timeout)
  {
    List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> polledRecords = new ArrayList<>();
    for (ConsumerRecord<byte[], byte[]> record : consumer.poll(Duration.ofMillis(timeout))) {

      polledRecords.add(new OrderedPartitionableRecord<>(
          record.topic(),
          new KafkaTopicPartition(multiTopic, record.topic(), record.partition()),
          record.offset(),
          record.value() == null ? null : ImmutableList.of(new KafkaRecordEntity(record))
      ));
    }
    return polledRecords;
  }

  @Override
  public Long getLatestSequenceNumber(StreamPartition<KafkaTopicPartition> partition)
  {
    Long currPos = getPosition(partition);
    seekToLatest(Collections.singleton(partition));
    Long nextPos = getPosition(partition);
    seek(partition, currPos);
    return nextPos;
  }

  @Override
  public Long getEarliestSequenceNumber(StreamPartition<KafkaTopicPartition> partition)
  {
    Long currPos = getPosition(partition);
    seekToEarliest(Collections.singleton(partition));
    Long nextPos = getPosition(partition);
    seek(partition, currPos);
    return nextPos;
  }

  @Override
  public boolean isOffsetAvailable(StreamPartition<KafkaTopicPartition> partition, OrderedSequenceNumber<Long> offset)
  {
    final Long earliestOffset = getEarliestSequenceNumber(partition);
    return earliestOffset != null
           && offset.isAvailableWithEarliest(KafkaSequenceNumber.of(earliestOffset));
  }

  @Override
  public Long getPosition(StreamPartition<KafkaTopicPartition> partition)
  {
    return wrapExceptions(() -> consumer.position(partition.getPartitionId().asTopicPartition(partition.getStream())));
  }

  @Override
  public Set<KafkaTopicPartition> getPartitionIds(String stream)
  {
    return wrapExceptions(() -> {
      List<PartitionInfo> allPartitions;
      if (multiTopic) {
        Pattern pattern = Pattern.compile(stream);
        allPartitions = consumer.listTopics()
                                .entrySet()
                                .stream()
                                .filter(e -> pattern.matcher(e.getKey()).matches())
                                .flatMap(e -> e.getValue().stream())
                                .collect(Collectors.toList());
        if (allPartitions.isEmpty()) {
          throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                              .ofCategory(DruidException.Category.INVALID_INPUT)
                              .build("No partitions found for topics that match given pattern [%s]."
                                     + "Check that the pattern regex is correct and matching topics exists", stream);
        }
      } else {
        allPartitions = consumer.partitionsFor(stream);
        if (allPartitions == null) {
          throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                              .ofCategory(DruidException.Category.INVALID_INPUT)
                              .build("Topic [%s] is not found."
                                     + " Check that the topic exists in Kafka cluster", stream);
        }
      }
      return allPartitions.stream()
                          .map(p -> new KafkaTopicPartition(multiTopic, p.topic(), p.partition()))
                          .collect(Collectors.toSet());
    });
  }

  /**
   * Returns a Monitor that emits Kafka consumer metrics.
   */
  public Monitor monitor()
  {
    return monitor;
  }

  @Override
  public void close()
  {
    if (closed) {
      return;
    }
    closed = true;

    monitor.stopAfterNextEmit();
    consumer.close();
  }

  public static void addConsumerPropertiesFromConfig(
      Properties properties,
      ObjectMapper configMapper,
      Map<String, Object> consumerProperties
  )
  {
    // Extract passwords before SSL connection to Kafka
    for (Map.Entry<String, Object> entry : consumerProperties.entrySet()) {
      String propertyKey = entry.getKey();

      if (!KafkaSupervisorIOConfig.DRUID_DYNAMIC_CONFIG_PROVIDER_KEY.equals(propertyKey)) {
        if (propertyKey.equals(KafkaSupervisorIOConfig.TRUST_STORE_PASSWORD_KEY)
            || propertyKey.equals(KafkaSupervisorIOConfig.KEY_STORE_PASSWORD_KEY)
            || propertyKey.equals(KafkaSupervisorIOConfig.KEY_PASSWORD_KEY)) {
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
    Object dynamicConfigProviderJson = consumerProperties.get(KafkaSupervisorIOConfig.DRUID_DYNAMIC_CONFIG_PROVIDER_KEY);
    if (dynamicConfigProviderJson != null) {
      DynamicConfigProvider dynamicConfigProvider = configMapper.convertValue(dynamicConfigProviderJson, DynamicConfigProvider.class);
      Map<String, String> dynamicConfig = dynamicConfigProvider.getConfig();
      for (Map.Entry<String, String> e : dynamicConfig.entrySet()) {
        properties.setProperty(e.getKey(), e.getValue());
      }
    }
  }

  private static Deserializer getKafkaDeserializer(Properties properties, String kafkaConfigKey, boolean isKey)
  {
    Deserializer deserializerObject;
    try {
      Class deserializerClass = Class.forName(properties.getProperty(
          kafkaConfigKey,
          ByteArrayDeserializer.class.getTypeName()
      ));
      Method deserializerMethod = deserializerClass.getMethod("deserialize", String.class, byte[].class);

      Type deserializerReturnType = deserializerMethod.getGenericReturnType();

      if (deserializerReturnType == byte[].class) {
        deserializerObject = (Deserializer) deserializerClass.getConstructor().newInstance();
      } else {
        throw new IllegalArgumentException("Kafka deserializers must return a byte array (byte[]), " +
                                           deserializerClass.getName() + " returns " +
                                           deserializerReturnType.getTypeName());
      }
    }
    catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new StreamException(e);
    }

    Map<String, Object> configs = new HashMap<>();
    for (String key : properties.stringPropertyNames()) {
      configs.put(key, properties.getProperty(key));
    }

    deserializerObject.configure(configs, isKey);
    return deserializerObject;
  }

  public static KafkaConsumer<byte[], byte[]> getKafkaConsumer(
      ObjectMapper sortingMapper,
      Map<String, Object> consumerProperties,
      KafkaConfigOverrides configOverrides
  )
  {
    final Map<String, Object> consumerConfigs = KafkaConsumerConfigs.getConsumerProperties();
    final Properties props = new Properties();
    Map<String, Object> effectiveConsumerProperties;
    if (configOverrides != null) {
      effectiveConsumerProperties = configOverrides.overrideConfigs(consumerProperties);
    } else {
      effectiveConsumerProperties = consumerProperties;
    }
    addConsumerPropertiesFromConfig(
        props,
        sortingMapper,
        effectiveConsumerProperties
    );
    props.putIfAbsent("isolation.level", "read_committed");
    props.putIfAbsent("group.id", StringUtils.format("kafka-supervisor-%s", IdUtils.getRandomId()));
    props.putAll(consumerConfigs);

    ClassLoader currCtxCl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(KafkaRecordSupplier.class.getClassLoader());
      Deserializer keyDeserializerObject = getKafkaDeserializer(props, "key.deserializer", true);
      Deserializer valueDeserializerObject = getKafkaDeserializer(props, "value.deserializer", false);

      return new KafkaConsumer<>(props, keyDeserializerObject, valueDeserializerObject);
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
