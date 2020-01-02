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

package org.apache.druid.indexing.pubsub.supervisor;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.supervisor.Supervisor;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManagerConfig;
import org.apache.druid.indexing.pubsub.PubsubIndexTaskClientFactory;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.server.metrics.DruidMonitorSchedulerConfig;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

public class PubsubSupervisorSpec implements SupervisorSpec
{
  private static final Logger log = new Logger(PubsubSupervisorSpec.class);
  private static final String TASK_TYPE = "pubsub";
  protected final TaskStorage taskStorage;
  protected final TaskMaster taskMaster;
  protected final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
  protected final PubsubIndexTaskClientFactory indexTaskClientFactory;
  protected final ObjectMapper mapper;
  protected final RowIngestionMetersFactory rowIngestionMetersFactory;
  protected final ServiceEmitter emitter;
  protected final DruidMonitorSchedulerConfig monitorSchedulerConfig;
  protected final SupervisorStateManagerConfig supervisorStateManagerConfig;
  private final PubsubSupervisorIngestionSpec ingestionSchema;
  private final PubsubSupervisorIOConfig ioConfig;
  private final PubsubSupervisorTuningConfig tuningConfig;
  @Nullable
  private final Map<String, Object> context;
  private final boolean suspended;

  @JsonCreator
  public PubsubSupervisorSpec(
      @JsonProperty("spec") @Nullable PubsubSupervisorIngestionSpec ingestionSchema,
      @JsonProperty("dataSchema") @Nullable DataSchema dataSchema,
      @JsonProperty("tuningConfig") @Nullable PubsubSupervisorTuningConfig tuningConfig,
      @JsonProperty("ioConfig") @Nullable PubsubSupervisorIOConfig ioConfig,
      @JsonProperty("context") Map<String, Object> context,
      @JsonProperty("suspended") Boolean suspended,
      @JacksonInject TaskStorage taskStorage,
      @JacksonInject TaskMaster taskMaster,
      @JacksonInject IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
      @JacksonInject PubsubIndexTaskClientFactory pubsubIndexTaskClientFactory,
      @JacksonInject @Json ObjectMapper mapper,
      @JacksonInject ServiceEmitter emitter,
      @JacksonInject DruidMonitorSchedulerConfig monitorSchedulerConfig,
      @JacksonInject RowIngestionMetersFactory rowIngestionMetersFactory,
      @JacksonInject SupervisorStateManagerConfig supervisorStateManagerConfig
  )
  {
    this.ingestionSchema = ingestionSchema != null
                           ? ingestionSchema
                           : new PubsubSupervisorIngestionSpec(
                               dataSchema,
                               ioConfig,
                               tuningConfig != null
                               ? tuningConfig
                               : PubsubSupervisorTuningConfig.defaultConfig()
                           );
    log.error("loggin spec");
    log.error(this.ingestionSchema.toString());
    log.error(this.ingestionSchema.getDataSchema().toString());
    this.context = context;

    this.taskStorage = taskStorage;
    this.taskMaster = taskMaster;
    this.indexerMetadataStorageCoordinator = indexerMetadataStorageCoordinator;
    this.indexTaskClientFactory = pubsubIndexTaskClientFactory;
    this.mapper = mapper;
    this.emitter = emitter;
    this.monitorSchedulerConfig = monitorSchedulerConfig;
    this.rowIngestionMetersFactory = rowIngestionMetersFactory;
    this.suspended = suspended != null ? suspended : false;
    this.supervisorStateManagerConfig = supervisorStateManagerConfig;
    this.ioConfig = ioConfig;
    this.tuningConfig = tuningConfig;
  }

  private static PubsubSupervisorIngestionSpec checkIngestionSchema(
      PubsubSupervisorIngestionSpec ingestionSchema
  )
  {
    Preconditions.checkNotNull(ingestionSchema, "ingestionSchema");
    Preconditions.checkNotNull(ingestionSchema.getDataSchema(), "dataSchema");
    Preconditions.checkNotNull(ingestionSchema.getIOConfig(), "ioConfig");
    return ingestionSchema;
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

  public DataSchema getDataSchema()
  {
    return ingestionSchema.getDataSchema();
  }

  public Map<String, Object> getContext()
  {
    return context;
  }

  @Override
  public List<String> getDataSources()
  {
    return ImmutableList.of(ingestionSchema.getDataSchema().getDataSource());
  }

  @Override
  public PubsubSupervisorSpec createSuspendedSpec()
  {
    return toggleSuspend(true);
  }

  @Override
  public PubsubSupervisorSpec createRunningSpec()
  {
    return toggleSuspend(false);
  }

  @Override
  public String getType()
  {
    return TASK_TYPE;
  }

  @Override
  public String getSource()
  {
    return getIoConfig() != null ? getIoConfig().getSubscription() : null;
  }

  @Override
  public boolean isSuspended()
  {
    return suspended;
  }

  public ServiceEmitter getEmitter()
  {
    return emitter;
  }

  public SupervisorStateManagerConfig getSupervisorStateManagerConfig()
  {
    return supervisorStateManagerConfig;
  }


  @Override
  public Supervisor createSupervisor()
  {
    return new PubsubSupervisor(
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        indexTaskClientFactory,
        mapper,
        this,
        rowIngestionMetersFactory
    );
  }

  @Deprecated
  @JsonProperty
  public PubsubSupervisorTuningConfig getTuningConfig()
  {
    return tuningConfig;
  }

  @Deprecated
  @JsonProperty
  public PubsubSupervisorIOConfig getIoConfig()
  {
    return ioConfig;
  }

  @JsonProperty
  public PubsubSupervisorIngestionSpec getSpec()
  {
    return ingestionSchema;
  }

  protected PubsubSupervisorSpec toggleSuspend(boolean suspend)
  {
    return new PubsubSupervisorSpec(
        getSpec(),
        getDataSchema(),
        getTuningConfig(),
        getIoConfig(),
        getContext(),
        suspend,
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        indexTaskClientFactory,
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
    return "PubsubSupervisorSpec{" +
           "dataSchema=" + getDataSchema() +
           ", tuningConfig=" + getTuningConfig() +
           ", ioConfig=" + getIoConfig() +
           ", context=" + getContext() +
           ", suspend=" + isSuspended() +
           '}';
  }
}
