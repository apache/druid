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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTask;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisor;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

public class KafkaIndexTask extends SeekableStreamIndexTask<Integer, Long>
{
  private static final String TYPE = "index_kafka";

  private final KafkaIndexTaskIOConfig ioConfig;
  private final ObjectMapper configMapper;

  // This value can be tuned in some tests
  private long pollRetryMs = 30000;

  @JsonCreator
  public KafkaIndexTask(
      @JsonProperty("id") String id,
      @JsonProperty("resource") TaskResource taskResource,
      @JsonProperty("dataSchema") DataSchema dataSchema,
      @JsonProperty("tuningConfig") KafkaIndexTaskTuningConfig tuningConfig,
      @JsonProperty("ioConfig") KafkaIndexTaskIOConfig ioConfig,
      @JsonProperty("context") Map<String, Object> context,
      @JacksonInject ChatHandlerProvider chatHandlerProvider,
      @JacksonInject AuthorizerMapper authorizerMapper,
      @JacksonInject RowIngestionMetersFactory rowIngestionMetersFactory,
      @JacksonInject ObjectMapper configMapper
  )
  {
    super(
        id == null ? getFormattedId(dataSchema.getDataSource(), TYPE) : id,
        taskResource,
        dataSchema,
        tuningConfig,
        ioConfig,
        context,
        chatHandlerProvider,
        authorizerMapper,
        rowIngestionMetersFactory,
        getFormattedGroupId(dataSchema.getDataSource(), TYPE)
    );
    this.configMapper = configMapper;
    this.ioConfig = ioConfig;

  }

  long getPollRetryMs()
  {
    return pollRetryMs;
  }

  @Deprecated
  KafkaConsumer<byte[], byte[]> newConsumer()
  {
    ClassLoader currCtxCl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

      final Properties props = new Properties();

      KafkaRecordSupplier.addConsumerPropertiesFromConfig(
          props,
          configMapper,
          ioConfig.getConsumerProperties()
      );

      props.setProperty("enable.auto.commit", "false");
      props.setProperty("auto.offset.reset", "none");
      props.setProperty("key.deserializer", ByteArrayDeserializer.class.getName());
      props.setProperty("value.deserializer", ByteArrayDeserializer.class.getName());

      return new KafkaConsumer<>(props);
    }
    finally {
      Thread.currentThread().setContextClassLoader(currCtxCl);
    }
  }

  @Deprecated
  static void assignPartitions(
      final KafkaConsumer consumer,
      final String topic,
      final Set<Integer> partitions
  )
  {
    consumer.assign(
        new ArrayList<>(
            partitions.stream().map(n -> new TopicPartition(topic, n)).collect(Collectors.toList())
        )
    );
  }

  @Override
  protected SeekableStreamIndexTaskRunner<Integer, Long> createTaskRunner()
  {
    if (context != null && context.get(SeekableStreamSupervisor.IS_INCREMENTAL_HANDOFF_SUPPORTED) != null
        && ((boolean) context.get(SeekableStreamSupervisor.IS_INCREMENTAL_HANDOFF_SUPPORTED))) {
      return new IncrementalPublishingKafkaIndexTaskRunner(
          this,
          parser,
          authorizerMapper,
          chatHandlerProvider,
          savedParseExceptions,
          rowIngestionMetersFactory
      );
    } else {
      return new LegacyKafkaIndexTaskRunner(
          this,
          parser,
          authorizerMapper,
          chatHandlerProvider,
          savedParseExceptions,
          rowIngestionMetersFactory
      );
    }
  }

  @Override
  protected KafkaRecordSupplier newTaskRecordSupplier()
  {
    ClassLoader currCtxCl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

      final Map<String, Object> props = new HashMap<>(((KafkaIndexTaskIOConfig) super.ioConfig).getConsumerProperties());

      props.put("auto.offset.reset", "none");
      props.put("key.deserializer", ByteArrayDeserializer.class.getName());
      props.put("value.deserializer", ByteArrayDeserializer.class.getName());

      return new KafkaRecordSupplier(props, configMapper);
    }
    finally {
      Thread.currentThread().setContextClassLoader(currCtxCl);
    }
  }

  @Override
  @JsonProperty
  public KafkaIndexTaskTuningConfig getTuningConfig()
  {
    return (KafkaIndexTaskTuningConfig) super.getTuningConfig();
  }

  @VisibleForTesting
  void setPollRetryMs(long retryMs)
  {
    this.pollRetryMs = retryMs;
  }

  @Override
  @JsonProperty("ioConfig")
  public KafkaIndexTaskIOConfig getIOConfig()
  {
    return (KafkaIndexTaskIOConfig) super.getIOConfig();
  }

  @Override
  public String getType()
  {
    return TYPE;
  }
}
