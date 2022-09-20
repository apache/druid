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
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTask;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner;
import org.apache.druid.segment.indexing.DataSchema;

import java.util.HashMap;
import java.util.Map;

public class KafkaIndexTask extends SeekableStreamIndexTask<Integer, Long, KafkaRecordEntity>
{
  private static final String TYPE = "index_kafka";

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
      @JacksonInject ObjectMapper configMapper
  )
  {
    super(
        getOrMakeId(id, dataSchema.getDataSource(), TYPE),
        taskResource,
        dataSchema,
        tuningConfig,
        ioConfig,
        context,
        getFormattedGroupId(dataSchema.getDataSource(), TYPE)
    );
    this.configMapper = configMapper;

    Preconditions.checkArgument(
        ioConfig.getStartSequenceNumbers().getExclusivePartitions().isEmpty(),
        "All startSequenceNumbers must be inclusive"
    );
  }

  long getPollRetryMs()
  {
    return pollRetryMs;
  }

  @Override
  protected SeekableStreamIndexTaskRunner<Integer, Long, KafkaRecordEntity> createTaskRunner()
  {
    //noinspection unchecked
    return new IncrementalPublishingKafkaIndexTaskRunner(
        this,
        dataSchema.getParser(),
        authorizerMapper,
        lockGranularityToUse
    );
  }

  @Override
  protected KafkaRecordSupplier newTaskRecordSupplier()
  {
    ClassLoader currCtxCl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
      KafkaIndexTaskIOConfig kafkaIndexTaskIOConfig = (KafkaIndexTaskIOConfig) super.ioConfig;
      final Map<String, Object> props = new HashMap<>(kafkaIndexTaskIOConfig.getConsumerProperties());

      props.put("auto.offset.reset", "none");

      return new KafkaRecordSupplier(props, configMapper, kafkaIndexTaskIOConfig.getConfigOverrides());
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

  @Override
  public boolean supportsQueries()
  {
    return true;
  }
}
