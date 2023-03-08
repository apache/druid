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

package org.apache.druid.indexing.rabbitstream.supervisor;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.supervisor.Supervisor;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManagerConfig;
import org.apache.druid.indexing.rabbitstream.RabbitStreamIndexTaskClientFactory;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorSpec;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.metrics.DruidMonitorSchedulerConfig;
import org.apache.druid.segment.incremental.RowIngestionMetersFactory;
import org.apache.druid.segment.indexing.DataSchema;

import javax.annotation.Nullable;

import java.util.Map;

public class RabbitStreamSupervisorSpec extends SeekableStreamSupervisorSpec
{
  private static final String TASK_TYPE = "rabbit";

  @JsonCreator
  public RabbitStreamSupervisorSpec(
      @JsonProperty("spec") @Nullable RabbitStreamSupervisorIngestionSpec ingestionSchema,
      @JsonProperty("dataSchema") @Nullable DataSchema dataSchema,
      @JsonProperty("tuningConfig") @Nullable RabbitStreamSupervisorTuningConfig tuningConfig,
      @JsonProperty("ioConfig") @Nullable RabbitStreamSupervisorIOConfig ioConfig,
      @JsonProperty("context") Map<String, Object> context,
      @JsonProperty("suspended") Boolean suspended,
      @JacksonInject TaskStorage taskStorage,
      @JacksonInject TaskMaster taskMaster,
      @JacksonInject IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
      @JacksonInject RabbitStreamIndexTaskClientFactory rabbitStreamIndexTaskClientFactory,
      @JacksonInject @Json ObjectMapper mapper,
      @JacksonInject ServiceEmitter emitter,
      @JacksonInject DruidMonitorSchedulerConfig monitorSchedulerConfig,
      @JacksonInject RowIngestionMetersFactory rowIngestionMetersFactory,
      @JacksonInject SupervisorStateManagerConfig supervisorStateManagerConfig)
  {
    super(
        ingestionSchema != null
            ? ingestionSchema
            : new RabbitStreamSupervisorIngestionSpec(
                dataSchema,
                ioConfig,
                tuningConfig != null
                    ? tuningConfig
                    : RabbitStreamSupervisorTuningConfig.defaultConfig()),
        context,
        suspended,
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        rabbitStreamIndexTaskClientFactory,
        mapper,
        emitter,
        monitorSchedulerConfig,
        rowIngestionMetersFactory,
        supervisorStateManagerConfig);
  }

  @Override
  public String getType()
  {
    return TASK_TYPE;
  }

  @Override
  public String getSource()
  {
    return getIoConfig() != null ? getIoConfig().getStream() : null;
  }

  @Override
  public Supervisor createSupervisor()
  {
    return new RabbitStreamSupervisor(
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        (RabbitStreamIndexTaskClientFactory) indexTaskClientFactory,
        mapper,
        this,
        rowIngestionMetersFactory);
  }

  @Override
  @Deprecated
  @JsonProperty
  public RabbitStreamSupervisorTuningConfig getTuningConfig()
  {
    return (RabbitStreamSupervisorTuningConfig) super.getTuningConfig();
  }

  @Override
  @Deprecated
  @JsonProperty
  public RabbitStreamSupervisorIOConfig getIoConfig()
  {
    return (RabbitStreamSupervisorIOConfig) super.getIoConfig();
  }

  @Override
  @JsonProperty
  public RabbitStreamSupervisorIngestionSpec getSpec()
  {
    return (RabbitStreamSupervisorIngestionSpec) super.getSpec();
  }

  @Override
  protected RabbitStreamSupervisorSpec toggleSuspend(boolean suspend)
  {
    return new RabbitStreamSupervisorSpec(
        getSpec(),
        getDataSchema(),
        getTuningConfig(),
        getIoConfig(),
        getContext(),
        suspend,
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        (RabbitStreamIndexTaskClientFactory) indexTaskClientFactory,
        mapper,
        emitter,
        monitorSchedulerConfig,
        rowIngestionMetersFactory,
        supervisorStateManagerConfig);
  }

  @Override
  public String toString()
  {
    return "RabbitStreamSupervisorSpec{" +
        "dataSchema=" + getDataSchema() +
        ", tuningConfig=" + getTuningConfig() +
        ", ioConfig=" + getIoConfig() +
        ", context=" + getContext() +
        ", suspend=" + isSuspended() +
        '}';
  }
}
