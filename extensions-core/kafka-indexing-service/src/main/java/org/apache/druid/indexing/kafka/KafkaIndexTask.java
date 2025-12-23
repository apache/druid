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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.druid.common.config.Configs;
import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.data.input.kafka.KafkaTopicPartition;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTask;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.ResourceAction;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class KafkaIndexTask extends SeekableStreamIndexTask<KafkaTopicPartition, Long, KafkaRecordEntity>
{
  private static final String TYPE = "index_kafka";

  /**
   * Resources that a {@link KafkaIndexTask} is authorized to use. Includes
   * performing a read action on an external resource of type
   */
  public static final Set<ResourceAction> INPUT_SOURCE_RESOURCES = Set.of(
      AuthorizationUtils.createExternalResourceReadAction(KafkaIndexTaskModule.SCHEME)
  );

  private final ObjectMapper configMapper;

  // This value can be tuned in some tests
  private long pollRetryMs = 30000;

  @JsonCreator
  public KafkaIndexTask(
      @JsonProperty("id") String id,
      @JsonProperty("supervisorId") @Nullable String supervisorId,
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
        supervisorId,
        taskResource,
        dataSchema,
        tuningConfig,
        ioConfig,
        context,
        getFormattedGroupId(Configs.valueOrDefault(supervisorId, dataSchema.getDataSource()), TYPE)
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
  protected SeekableStreamIndexTaskRunner<KafkaTopicPartition, Long, KafkaRecordEntity> createTaskRunner()
  {
    //noinspection unchecked
    return new KafkaIndexTaskRunner(
        this,
        dataSchema.getParser(),
        lockGranularityToUse
    );
  }

  @Override
  protected KafkaRecordSupplier newTaskRecordSupplier(final TaskToolbox toolbox)
  {
    ClassLoader currCtxCl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
      KafkaIndexTaskIOConfig kafkaIndexTaskIOConfig = (KafkaIndexTaskIOConfig) super.ioConfig;
      final Map<String, Object> props = new HashMap<>(kafkaIndexTaskIOConfig.getConsumerProperties());

      props.put("auto.offset.reset", "none");

      final KafkaRecordSupplier recordSupplier =
          new KafkaRecordSupplier(
              props,
              configMapper,
              kafkaIndexTaskIOConfig.getConfigOverrides(),
              kafkaIndexTaskIOConfig.isMultiTopic(),
              kafkaIndexTaskIOConfig.getheaderBasedFilterConfig()
          );

      if (toolbox.getMonitorScheduler() != null) {
        toolbox.getMonitorScheduler().addMonitor(recordSupplier.monitor());
      }

      return recordSupplier;
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

  @Nonnull
  @JsonIgnore
  @Override
  public Set<ResourceAction> getInputSourceResources()
  {
    return INPUT_SOURCE_RESOURCES;
  }

  @Override
  public boolean supportsQueries()
  {
    return true;
  }
}
