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

package org.apache.druid.indexing.seekablestream.supervisor;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.druid.annotations.SuppressFBWarnings;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.supervisor.Supervisor;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManagerConfig;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.DefaultAutoScaler;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.DummyAutoScaler;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.SupervisorTaskAutoscaler;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskClientFactory;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.segment.incremental.RowIngestionMetersFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.server.metrics.DruidMonitorSchedulerConfig;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

public abstract class SeekableStreamSupervisorSpec implements SupervisorSpec
{

  private static SeekableStreamSupervisorIngestionSpec checkIngestionSchema(
      SeekableStreamSupervisorIngestionSpec ingestionSchema
  )
  {
    Preconditions.checkNotNull(ingestionSchema, "ingestionSchema");
    Preconditions.checkNotNull(ingestionSchema.getDataSchema(), "dataSchema");
    Preconditions.checkNotNull(ingestionSchema.getIOConfig(), "ioConfig");
    return ingestionSchema;
  }

  protected final TaskStorage taskStorage;
  protected final TaskMaster taskMaster;
  protected final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
  protected final SeekableStreamIndexTaskClientFactory indexTaskClientFactory;
  protected final ObjectMapper mapper;
  protected final RowIngestionMetersFactory rowIngestionMetersFactory;
  private final SeekableStreamSupervisorIngestionSpec ingestionSchema;
  @Nullable
  private final Map<String, Object> context;
  protected final ServiceEmitter emitter;
  protected final DruidMonitorSchedulerConfig monitorSchedulerConfig;
  private final boolean suspended;
  protected final SupervisorStateManagerConfig supervisorStateManagerConfig;

  @JsonCreator
  public SeekableStreamSupervisorSpec(
      @JsonProperty("spec") final SeekableStreamSupervisorIngestionSpec ingestionSchema,
      @JsonProperty("context") @Nullable Map<String, Object> context,
      @JsonProperty("suspended") Boolean suspended,
      @JacksonInject TaskStorage taskStorage,
      @JacksonInject TaskMaster taskMaster,
      @JacksonInject IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
      @JacksonInject SeekableStreamIndexTaskClientFactory indexTaskClientFactory,
      @JacksonInject @Json ObjectMapper mapper,
      @JacksonInject ServiceEmitter emitter,
      @JacksonInject DruidMonitorSchedulerConfig monitorSchedulerConfig,
      @JacksonInject RowIngestionMetersFactory rowIngestionMetersFactory,
      @JacksonInject SupervisorStateManagerConfig supervisorStateManagerConfig
  )
  {
    this.ingestionSchema = checkIngestionSchema(ingestionSchema);
    this.context = context;

    this.taskStorage = taskStorage;
    this.taskMaster = taskMaster;
    this.indexerMetadataStorageCoordinator = indexerMetadataStorageCoordinator;
    this.indexTaskClientFactory = indexTaskClientFactory;
    this.mapper = mapper;
    this.emitter = emitter;
    this.monitorSchedulerConfig = monitorSchedulerConfig;
    this.rowIngestionMetersFactory = rowIngestionMetersFactory;
    this.suspended = suspended != null ? suspended : false;
    this.supervisorStateManagerConfig = supervisorStateManagerConfig;
  }

  @JsonProperty
  public SeekableStreamSupervisorIngestionSpec getSpec()
  {
    return ingestionSchema;
  }

  @Deprecated
  @JsonProperty
  public DataSchema getDataSchema()
  {
    return ingestionSchema.getDataSchema();
  }

  @JsonProperty
  public SeekableStreamSupervisorTuningConfig getTuningConfig()
  {
    return ingestionSchema.getTuningConfig();
  }

  @JsonProperty
  public SeekableStreamSupervisorIOConfig getIoConfig()
  {
    return ingestionSchema.getIOConfig();
  }

  @Nullable
  @JsonProperty
  public Map<String, Object> getContext()
  {
    return context;
  }

  public ServiceEmitter getEmitter()
  {
    return emitter;
  }

  @Override
  public String getId()
  {
    return ingestionSchema.getDataSchema().getDataSource();
  }

  public DruidMonitorSchedulerConfig getMonitorSchedulerConfig()
  {
    return monitorSchedulerConfig;
  }

  @Override
  public abstract Supervisor createSupervisor();

  /**
   * need to notice that autoScaler would be null which means autoscale is dissable.
   * @param supervisor
   * @return autoScaler, disable autoscale will return dummyAutoScaler and enable autoscale wiil return defaultAutoScaler by default.
   */
  @Override
  @SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED", justification = "using siwtch(String)")
  public SupervisorTaskAutoscaler createAutoscaler(Supervisor supervisor)
  {
    String dataSource = getId();
    SupervisorTaskAutoscaler autoScaler = new DummyAutoScaler(supervisor, dataSource);
    Map<String, Object> dynamicAllocationTasksProperties = ingestionSchema.getIOConfig().getDynamicAllocationTasksProperties();
    if (dynamicAllocationTasksProperties != null && !dynamicAllocationTasksProperties.isEmpty() && Boolean.parseBoolean(String.valueOf(dynamicAllocationTasksProperties.getOrDefault("enableDynamicAllocationTasks", false)))) {
      String autoScalerStrategy = String.valueOf(dynamicAllocationTasksProperties.getOrDefault("autoScalerStrategy", "default"));

      // will thorw 'Return value of String.hashCode() ignored : RV_RETURN_VALUE_IGNORED' just Suppress it.
      switch (StringUtils.toLowerCase(autoScalerStrategy)) {
        default: autoScaler = new DefaultAutoScaler(supervisor, dataSource, dynamicAllocationTasksProperties, this);
      }
    }
    return autoScaler;
  }

  @Override
  public List<String> getDataSources()
  {
    return ImmutableList.of(getDataSchema().getDataSource());
  }

  @Override
  public SeekableStreamSupervisorSpec createSuspendedSpec()
  {
    return toggleSuspend(true);
  }

  @Override
  public SeekableStreamSupervisorSpec createRunningSpec()
  {
    return toggleSuspend(false);
  }

  public SupervisorStateManagerConfig getSupervisorStateManagerConfig()
  {
    return supervisorStateManagerConfig;
  }

  @Override
  @JsonProperty("suspended")
  public boolean isSuspended()
  {
    return suspended;
  }

  protected abstract SeekableStreamSupervisorSpec toggleSuspend(boolean suspend);

}
