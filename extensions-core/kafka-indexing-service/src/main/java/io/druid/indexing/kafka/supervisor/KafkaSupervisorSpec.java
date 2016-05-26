/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.kafka.supervisor;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import io.druid.guice.annotations.Json;
import io.druid.indexing.kafka.KafkaIndexTaskClient;
import io.druid.indexing.kafka.KafkaIndexTaskClientFactory;
import io.druid.indexing.kafka.KafkaTuningConfig;
import io.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import io.druid.indexing.overlord.TaskMaster;
import io.druid.indexing.overlord.TaskStorage;
import io.druid.indexing.overlord.supervisor.Supervisor;
import io.druid.indexing.overlord.supervisor.SupervisorSpec;
import io.druid.segment.indexing.DataSchema;

public class KafkaSupervisorSpec implements SupervisorSpec
{
  private final DataSchema dataSchema;
  private final KafkaTuningConfig tuningConfig;
  private final KafkaSupervisorIOConfig ioConfig;

  private final TaskStorage taskStorage;
  private final TaskMaster taskMaster;
  private final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
  private final KafkaIndexTaskClientFactory kafkaIndexTaskClientFactory;
  private final ObjectMapper mapper;

  @JsonCreator
  public KafkaSupervisorSpec(
      @JsonProperty("dataSchema") DataSchema dataSchema,
      @JsonProperty("tuningConfig") KafkaTuningConfig tuningConfig,
      @JsonProperty("ioConfig") KafkaSupervisorIOConfig ioConfig,
      @JacksonInject TaskStorage taskStorage,
      @JacksonInject TaskMaster taskMaster,
      @JacksonInject IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
      @JacksonInject KafkaIndexTaskClientFactory kafkaIndexTaskClientFactory,
      @JacksonInject @Json ObjectMapper mapper
  )
  {
    this.dataSchema = Preconditions.checkNotNull(dataSchema, "dataSchema");
    this.tuningConfig = tuningConfig != null
                        ? tuningConfig
                        : new KafkaTuningConfig(null, null, null, null, null, null, null, null, null);
    this.ioConfig = Preconditions.checkNotNull(ioConfig, "ioConfig");

    this.taskStorage = taskStorage;
    this.taskMaster = taskMaster;
    this.indexerMetadataStorageCoordinator = indexerMetadataStorageCoordinator;
    this.kafkaIndexTaskClientFactory = kafkaIndexTaskClientFactory;
    this.mapper = mapper;
  }

  @JsonProperty
  public DataSchema getDataSchema()
  {
    return dataSchema;
  }

  @JsonProperty
  public KafkaTuningConfig getTuningConfig()
  {
    return tuningConfig;
  }

  @JsonProperty
  public KafkaSupervisorIOConfig getIoConfig()
  {
    return ioConfig;
  }

  @Override
  public String getId()
  {
    return dataSchema.getDataSource();
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
        this
    );
  }
}
