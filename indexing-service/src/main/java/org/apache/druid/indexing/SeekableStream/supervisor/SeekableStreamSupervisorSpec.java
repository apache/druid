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

package org.apache.druid.indexing.SeekableStream.supervisor;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.indexing.SeekableStream.SeekableStreamIndexTaskClientFactory;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
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

abstract public class SeekableStreamSupervisorSpec implements SupervisorSpec
{
  private final DataSchema dataSchema;
  private final SeekableStreamSupervisorTuningConfig tuningConfig;
  private final SeekableStreamSupervisorIOConfig ioConfig;
  private final Map<String, Object> context;
  private final ServiceEmitter emitter;
  private final DruidMonitorSchedulerConfig monitorSchedulerConfig;

  protected final TaskStorage taskStorage;
  protected final TaskMaster taskMaster;
  protected final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
  protected final SeekableStreamIndexTaskClientFactory indexTaskClientFactory;
  protected final ObjectMapper mapper;
  protected final RowIngestionMetersFactory rowIngestionMetersFactory;

  @JsonCreator
  public SeekableStreamSupervisorSpec(
      @JsonProperty("dataSchema") DataSchema dataSchema,
      @JsonProperty("tuningConfig") SeekableStreamSupervisorTuningConfig tuningConfig,
      @JsonProperty("ioConfig") SeekableStreamSupervisorIOConfig ioConfig,
      @JsonProperty("context") Map<String, Object> context,
      @JacksonInject TaskStorage taskStorage,
      @JacksonInject TaskMaster taskMaster,
      @JacksonInject IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
      @JacksonInject SeekableStreamIndexTaskClientFactory indexTaskClientFactory,
      @JacksonInject @Json ObjectMapper mapper,
      @JacksonInject ServiceEmitter emitter,
      @JacksonInject DruidMonitorSchedulerConfig monitorSchedulerConfig,
      @JacksonInject RowIngestionMetersFactory rowIngestionMetersFactory
  )
  {
    this.dataSchema = Preconditions.checkNotNull(dataSchema, "dataSchema");
    this.tuningConfig = tuningConfig; // null check done in concrete class
    this.ioConfig = Preconditions.checkNotNull(ioConfig, "ioConfig");
    this.context = context;

    this.taskStorage = taskStorage;
    this.taskMaster = taskMaster;
    this.indexerMetadataStorageCoordinator = indexerMetadataStorageCoordinator;
    this.indexTaskClientFactory = indexTaskClientFactory;
    this.mapper = mapper;
    this.emitter = emitter;
    this.monitorSchedulerConfig = monitorSchedulerConfig;
    this.rowIngestionMetersFactory = rowIngestionMetersFactory;
  }

  @JsonProperty
  public DataSchema getDataSchema()
  {
    return dataSchema;
  }

  @JsonProperty
  public SeekableStreamSupervisorTuningConfig getTuningConfig()
  {
    return tuningConfig;
  }

  @JsonProperty
  public SeekableStreamSupervisorIOConfig getIoConfig()
  {
    return ioConfig;
  }

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
    return dataSchema.getDataSource();
  }

  public DruidMonitorSchedulerConfig getMonitorSchedulerConfig()
  {
    return monitorSchedulerConfig;
  }

  @Override
  abstract public Supervisor createSupervisor();

  @Override
  public List<String> getDataSources()
  {
    return ImmutableList.of(getDataSchema().getDataSource());
  }

  @Override
  abstract public String toString();


}
