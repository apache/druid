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

package org.apache.druid.indexing.kafka.supervisor;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.kafka.KafkaIndexTaskClientFactory;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.supervisor.Supervisor;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.server.metrics.DruidMonitorSchedulerConfig;

import java.util.List;
import java.util.Map;

public class KafkaSupervisorSpec implements SupervisorSpec
{
  private final DataSchema dataSchema;
  private final KafkaSupervisorTuningConfig tuningConfig;
  private final KafkaSupervisorIOConfig ioConfig;
  private final Map<String, Object> context;

  private final TaskStorage taskStorage;
  private final TaskMaster taskMaster;
  private final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
  private final KafkaIndexTaskClientFactory kafkaIndexTaskClientFactory;
  private final ObjectMapper mapper;
  private final ServiceEmitter emitter;
  private final DruidMonitorSchedulerConfig monitorSchedulerConfig;
  private final RowIngestionMetersFactory rowIngestionMetersFactory;
  private final boolean suspended;

  @JsonCreator
  public KafkaSupervisorSpec(
      @JsonProperty("dataSchema") DataSchema dataSchema,
      @JsonProperty("tuningConfig") KafkaSupervisorTuningConfig tuningConfig,
      @JsonProperty("ioConfig") KafkaSupervisorIOConfig ioConfig,
      @JsonProperty("context") Map<String, Object> context,
      @JsonProperty("suspended") Boolean suspended,
      @JacksonInject TaskStorage taskStorage,
      @JacksonInject TaskMaster taskMaster,
      @JacksonInject IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
      @JacksonInject KafkaIndexTaskClientFactory kafkaIndexTaskClientFactory,
      @JacksonInject @Json ObjectMapper mapper,
      @JacksonInject ServiceEmitter emitter,
      @JacksonInject DruidMonitorSchedulerConfig monitorSchedulerConfig,
      @JacksonInject RowIngestionMetersFactory rowIngestionMetersFactory
  )
  {
    this.dataSchema = Preconditions.checkNotNull(dataSchema, "dataSchema");
    this.tuningConfig = tuningConfig != null
                        ? tuningConfig
                        : new KafkaSupervisorTuningConfig(
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null
                        );
    this.ioConfig = Preconditions.checkNotNull(ioConfig, "ioConfig");
    this.context = context;

    this.taskStorage = taskStorage;
    this.taskMaster = taskMaster;
    this.indexerMetadataStorageCoordinator = indexerMetadataStorageCoordinator;
    this.kafkaIndexTaskClientFactory = kafkaIndexTaskClientFactory;
    this.mapper = mapper;
    this.emitter = emitter;
    this.monitorSchedulerConfig = monitorSchedulerConfig;
    this.rowIngestionMetersFactory = rowIngestionMetersFactory;
    this.suspended = suspended != null ? suspended : false;
  }

  @JsonProperty
  public DataSchema getDataSchema()
  {
    return dataSchema;
  }

  @JsonProperty
  public KafkaSupervisorTuningConfig getTuningConfig()
  {
    return tuningConfig;
  }

  @JsonProperty
  public KafkaSupervisorIOConfig getIoConfig()
  {
    return ioConfig;
  }

  @JsonProperty
  public Map<String, Object> getContext()
  {
    return context;
  }

  @Override
  @JsonProperty("suspended")
  public boolean isSuspended()
  {
    return suspended;
  }

  public ServiceEmitter getEmitter()
  {
    return emitter;
  }

  @Override
  public String getId()
  {
    return dataSchema.getDataSource();
  }

  public DruidMonitorSchedulerConfig getMonitorSchedulerConfig()
  {
    return monitorSchedulerConfig;
  }

  @Override
  public Supervisor createSupervisor()
  {
    return new KafkaSupervisor(
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        kafkaIndexTaskClientFactory,
        mapper,
        this,
        rowIngestionMetersFactory
    );
  }

  @Override
  public List<String> getDataSources()
  {
    return ImmutableList.of(getDataSchema().getDataSource());
  }

  @Override
  public String toString()
  {
    return "KafkaSupervisorSpec{" +
           "dataSchema=" + dataSchema +
           ", tuningConfig=" + tuningConfig +
           ", ioConfig=" + ioConfig +
           '}';
  }

  @Override
  public KafkaSupervisorSpec createSuspendedSpec()
  {
    return toggleSuspend(true);
  }

  @Override
  public KafkaSupervisorSpec createRunningSpec()
  {
    return toggleSuspend(false);
  }

  private KafkaSupervisorSpec toggleSuspend(boolean suspend)
  {
    return new KafkaSupervisorSpec(
        dataSchema,
        tuningConfig,
        ioConfig,
        context,
        suspend,
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        kafkaIndexTaskClientFactory,
        mapper,
        emitter,
        monitorSchedulerConfig,
        rowIngestionMetersFactory
    );
  }
}
