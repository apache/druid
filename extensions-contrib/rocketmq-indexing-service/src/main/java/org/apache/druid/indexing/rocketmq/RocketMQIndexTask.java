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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.rocketmq.RocketMQRecordEntity;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTask;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner;
import org.apache.druid.segment.indexing.DataSchema;

import java.util.HashMap;
import java.util.Map;

public class RocketMQIndexTask extends SeekableStreamIndexTask<String, Long, RocketMQRecordEntity>
{
  private static final String TYPE = "index_rocketmq";

  private final RocketMQIndexTaskIOConfig ioConfig;
  private final ObjectMapper configMapper;

  // This value can be tuned in some tests
  private long pollRetryMs = 30000;

  @JsonCreator
  public RocketMQIndexTask(
      @JsonProperty("id") String id,
      @JsonProperty("resource") TaskResource taskResource,
      @JsonProperty("dataSchema") DataSchema dataSchema,
      @JsonProperty("tuningConfig") RocketMQIndexTaskTuningConfig tuningConfig,
      @JsonProperty("ioConfig") RocketMQIndexTaskIOConfig ioConfig,
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
    this.ioConfig = ioConfig;

    Preconditions.checkArgument(
        ioConfig.getStartSequenceNumbers().getExclusivePartitions().isEmpty(),
        "All startSequenceNumbers must be inclusive"
    );
  }

  @Override
  protected SeekableStreamIndexTaskRunner<String, Long, RocketMQRecordEntity> createTaskRunner()
  {
    return new RocketMQIndexTaskRunner(
        this,
        dataSchema.getParser(),
        authorizerMapper,
        lockGranularityToUse
    );
  }

  @Override
  protected RocketMQRecordSupplier newTaskRecordSupplier()
  {
    ClassLoader currCtxCl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

      final Map<String, Object> props = new HashMap<>(((RocketMQIndexTaskIOConfig) super.ioConfig).getConsumerProperties());

      return new RocketMQRecordSupplier(props, configMapper);
    }
    finally {
      Thread.currentThread().setContextClassLoader(currCtxCl);
    }
  }

  @Override
  @JsonProperty
  public RocketMQIndexTaskTuningConfig getTuningConfig()
  {
    return (RocketMQIndexTaskTuningConfig) super.getTuningConfig();
  }

  @VisibleForTesting
  void setPollRetryMs(long retryMs)
  {
    this.pollRetryMs = retryMs;
  }

  @Override
  @JsonProperty("ioConfig")
  public RocketMQIndexTaskIOConfig getIOConfig()
  {
    return (RocketMQIndexTaskIOConfig) super.getIOConfig();
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
