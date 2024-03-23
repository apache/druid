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

package org.apache.druid.indexing.rabbitstream;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTask;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.utils.RuntimeInfo;

import java.util.HashMap;
import java.util.Map;

public class RabbitStreamIndexTask extends SeekableStreamIndexTask<String, Long, ByteEntity>
{
  private static final String TYPE = "index_rabbit";
  private final ObjectMapper configMapper;
  // This value can be tuned in some tests
  private long pollRetryMs = 30000;
  private RuntimeInfo runtimeInfo;

  @JsonCreator
  public RabbitStreamIndexTask(
      @JsonProperty("id") String id,
      @JsonProperty("resource") TaskResource taskResource,
      @JsonProperty("dataSchema") DataSchema dataSchema,
      @JsonProperty("tuningConfig") RabbitStreamIndexTaskTuningConfig tuningConfig,
      @JsonProperty("ioConfig") RabbitStreamIndexTaskIOConfig ioConfig,
      @JsonProperty("context") Map<String, Object> context,
      @JacksonInject ObjectMapper configMapper)
  {
    super(
        getOrMakeId(id, dataSchema.getDataSource(), TYPE),
        taskResource,
        dataSchema,
        tuningConfig,
        ioConfig,
        context,
        getFormattedGroupId(dataSchema.getDataSource(), TYPE));
    this.configMapper = configMapper;

    Preconditions.checkArgument(
        ioConfig.getStartSequenceNumbers().getExclusivePartitions().isEmpty(),
        "All startSequenceNumbers must be inclusive");
  }

  long getPollRetryMs()
  {
    return pollRetryMs;
  }

  @Override
  public TaskStatus runTask(TaskToolbox toolbox)
  {
    this.runtimeInfo = toolbox.getAdjustedRuntimeInfo();
    return super.runTask(toolbox);
  }

  @Override
  protected SeekableStreamIndexTaskRunner<String, Long, ByteEntity> createTaskRunner()
  {
    // noinspection unchecked
    return new IncrementalPublishingRabbitStreamIndexTaskRunner(
        this,
        dataSchema.getParser(),
        authorizerMapper,
        lockGranularityToUse);
  }

  @Override
  protected RabbitStreamRecordSupplier newTaskRecordSupplier()
  {

    RabbitStreamIndexTaskIOConfig ioConfig = ((RabbitStreamIndexTaskIOConfig) super.ioConfig);
    RabbitStreamIndexTaskTuningConfig tuningConfig = ((RabbitStreamIndexTaskTuningConfig) super.tuningConfig);
    final Map<String, Object> props = new HashMap<>(ioConfig.getConsumerProperties());

    final int recordBufferSize =
        tuningConfig.getRecordBufferSizeOrDefault(runtimeInfo.getMaxHeapSizeBytes());
    final int maxRecordsPerPoll = tuningConfig.getMaxRecordsPerPollOrDefault();


    return new RabbitStreamRecordSupplier(
      props,
      configMapper,
      ioConfig.getUri(),
      recordBufferSize,
      tuningConfig.getRecordBufferOfferTimeout(),
      maxRecordsPerPoll
      );
  }

  @Override
  @JsonProperty
  public RabbitStreamIndexTaskTuningConfig getTuningConfig()
  {
    return (RabbitStreamIndexTaskTuningConfig) super.getTuningConfig();
  }

  @VisibleForTesting
  void setPollRetryMs(long retryMs)
  {
    this.pollRetryMs = retryMs;
  }

  @Override
  @JsonProperty("ioConfig")
  public RabbitStreamIndexTaskIOConfig getIOConfig()
  {
    return (RabbitStreamIndexTaskIOConfig) super.getIOConfig();
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
