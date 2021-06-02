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

package org.apache.druid.indexing.rocketmq.supervisor;

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
import org.apache.druid.indexing.rocketmq.RocketMQIndexTaskClientFactory;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorSpec;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.segment.incremental.RowIngestionMetersFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.server.metrics.DruidMonitorSchedulerConfig;

import javax.annotation.Nullable;
import java.util.Map;

public class RocketMQSupervisorSpec extends SeekableStreamSupervisorSpec
{
  private static final String TASK_TYPE = "rocketmq";

  @JsonCreator
  public RocketMQSupervisorSpec(
      @JsonProperty("spec") @Nullable RocketMQSupervisorIngestionSpec ingestionSchema,
      @JsonProperty("dataSchema") @Nullable DataSchema dataSchema,
      @JsonProperty("tuningConfig") @Nullable RocketMQSupervisorTuningConfig tuningConfig,
      @JsonProperty("ioConfig") @Nullable RocketMQSupervisorIOConfig ioConfig,
      @JsonProperty("context") Map<String, Object> context,
      @JsonProperty("suspended") Boolean suspended,
      @JacksonInject TaskStorage taskStorage,
      @JacksonInject TaskMaster taskMaster,
      @JacksonInject IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
      @JacksonInject RocketMQIndexTaskClientFactory rocketmqIndexTaskClientFactory,
      @JacksonInject @Json ObjectMapper mapper,
      @JacksonInject ServiceEmitter emitter,
      @JacksonInject DruidMonitorSchedulerConfig monitorSchedulerConfig,
      @JacksonInject RowIngestionMetersFactory rowIngestionMetersFactory,
      @JacksonInject SupervisorStateManagerConfig supervisorStateManagerConfig
  )
  {
    super(
        ingestionSchema != null
        ? ingestionSchema
        : new RocketMQSupervisorIngestionSpec(
            dataSchema,
            ioConfig,
            tuningConfig != null
            ? tuningConfig
            : RocketMQSupervisorTuningConfig.defaultConfig()
        ),
        context,
        suspended,
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        rocketmqIndexTaskClientFactory,
        mapper,
        emitter,
        monitorSchedulerConfig,
        rowIngestionMetersFactory,
        supervisorStateManagerConfig
    );
  }

  @Override
  public String getType()
  {
    return TASK_TYPE;
  }

  @Override
  public String getSource()
  {
    return getIoConfig() != null ? getIoConfig().getTopic() : null;
  }

  @Override
  public Supervisor createSupervisor()
  {
    return new RocketMQSupervisor(
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        (RocketMQIndexTaskClientFactory) indexTaskClientFactory,
        mapper,
        this,
        rowIngestionMetersFactory
    );
  }

  @Override
  @Deprecated
  @JsonProperty
  public RocketMQSupervisorTuningConfig getTuningConfig()
  {
    return (RocketMQSupervisorTuningConfig) super.getTuningConfig();
  }

  @Override
  @Deprecated
  @JsonProperty
  public RocketMQSupervisorIOConfig getIoConfig()
  {
    return (RocketMQSupervisorIOConfig) super.getIoConfig();
  }

  @Override
  @JsonProperty
  public RocketMQSupervisorIngestionSpec getSpec()
  {
    return (RocketMQSupervisorIngestionSpec) super.getSpec();
  }

  @Override
  protected RocketMQSupervisorSpec toggleSuspend(boolean suspend)
  {
    return new RocketMQSupervisorSpec(
        getSpec(),
        getDataSchema(),
        getTuningConfig(),
        getIoConfig(),
        getContext(),
        suspend,
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        (RocketMQIndexTaskClientFactory) indexTaskClientFactory,
        mapper,
        emitter,
        monitorSchedulerConfig,
        rowIngestionMetersFactory,
        supervisorStateManagerConfig
    );
  }

  @Override
  public String toString()
  {
    return "RocketMQSupervisorSpec{" +
           "dataSchema=" + getDataSchema() +
           ", tuningConfig=" + getTuningConfig() +
           ", ioConfig=" + getIoConfig() +
           ", context=" + getContext() +
           ", suspend=" + isSuspended() +
           '}';
  }
}
