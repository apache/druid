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

package org.apache.druid.indexing.pulsar.supervisor;

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
import org.apache.druid.indexing.pulsar.PulsarIndexTaskClientFactory;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorSpec;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.segment.incremental.RowIngestionMetersFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.server.metrics.DruidMonitorSchedulerConfig;

import javax.annotation.Nullable;
import java.util.Map;

public class PulsarSupervisorSpec extends SeekableStreamSupervisorSpec
{
  private static final String TASK_TYPE = "pulsar";

  @JsonCreator
  public PulsarSupervisorSpec(
      @JsonProperty("spec") @Nullable PulsarSupervisorIngestionSpec ingestionSchema,
      @JsonProperty("dataSchema") @Nullable DataSchema dataSchema,
      @JsonProperty("tuningConfig") @Nullable PulsarSupervisorTuningConfig tuningConfig,
      @JsonProperty("ioConfig") @Nullable PulsarSupervisorIOConfig ioConfig,
      @JsonProperty("context") Map<String, Object> context,
      @JsonProperty("suspended") Boolean suspended,
      @JacksonInject TaskStorage taskStorage,
      @JacksonInject TaskMaster taskMaster,
      @JacksonInject IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
      @JacksonInject PulsarIndexTaskClientFactory pulsarIndexTaskClientFactory,
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
        : new PulsarSupervisorIngestionSpec(
            dataSchema,
            ioConfig,
            tuningConfig != null
            ? tuningConfig
            : PulsarSupervisorTuningConfig.defaultConfig()
        ),
        context,
        suspended,
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        pulsarIndexTaskClientFactory,
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
    return new PulsarSupervisor(
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        (PulsarIndexTaskClientFactory) indexTaskClientFactory,
        mapper,
        this,
        rowIngestionMetersFactory
    );
  }

  @Override
  @Deprecated
  @JsonProperty
  public PulsarSupervisorTuningConfig getTuningConfig()
  {
    return (PulsarSupervisorTuningConfig) super.getTuningConfig();
  }

  @Override
  @Deprecated
  @JsonProperty
  public PulsarSupervisorIOConfig getIoConfig()
  {
    return (PulsarSupervisorIOConfig) super.getIoConfig();
  }

  @Override
  @JsonProperty
  public PulsarSupervisorIngestionSpec getSpec()
  {
    return (PulsarSupervisorIngestionSpec) super.getSpec();
  }

  @Override
  protected PulsarSupervisorSpec toggleSuspend(boolean suspend)
  {
    return new PulsarSupervisorSpec(
        getSpec(),
        getDataSchema(),
        getTuningConfig(),
        getIoConfig(),
        getContext(),
        suspend,
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        (PulsarIndexTaskClientFactory) indexTaskClientFactory,
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
    return "PulsarSupervisorSpec{" +
           "dataSchema=" + getDataSchema() +
           ", tuningConfig=" + getTuningConfig() +
           ", ioConfig=" + getIoConfig() +
           ", context=" + getContext() +
           ", suspend=" + isSuspended() +
           '}';
  }
}
