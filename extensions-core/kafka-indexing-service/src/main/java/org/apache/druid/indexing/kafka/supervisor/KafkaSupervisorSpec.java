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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.common.config.Configs;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.indexing.kafka.KafkaIndexTask;
import org.apache.druid.indexing.kafka.KafkaIndexTaskClientFactory;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.supervisor.Supervisor;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManagerConfig;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorSpec;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.metrics.DruidMonitorSchedulerConfig;
import org.apache.druid.segment.incremental.RowIngestionMetersFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.server.security.ResourceAction;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;

public class KafkaSupervisorSpec extends SeekableStreamSupervisorSpec
{
  static final String TASK_TYPE = "kafka";

  @JsonCreator
  public KafkaSupervisorSpec(
      @JsonProperty("id") @Nullable String id,
      @JsonProperty("spec") @Nullable KafkaSupervisorIngestionSpec ingestionSchema,
      @JsonProperty("dataSchema") @Nullable DataSchema dataSchema,
      @JsonProperty("tuningConfig") @Nullable KafkaSupervisorTuningConfig tuningConfig,
      @JsonProperty("ioConfig") @Nullable KafkaSupervisorIOConfig ioConfig,
      @JsonProperty("context") Map<String, Object> context,
      @JsonProperty("suspended") Boolean suspended,
      @JsonProperty("usePerpetuallyRunningTasks") @Nullable Boolean usePerpetuallyRunningTasks,
      @JacksonInject TaskStorage taskStorage,
      @JacksonInject TaskMaster taskMaster,
      @JacksonInject IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
      @JacksonInject KafkaIndexTaskClientFactory kafkaIndexTaskClientFactory,
      @JacksonInject @Json ObjectMapper mapper,
      @JacksonInject ServiceEmitter emitter,
      @JacksonInject DruidMonitorSchedulerConfig monitorSchedulerConfig,
      @JacksonInject RowIngestionMetersFactory rowIngestionMetersFactory,
      @JacksonInject SupervisorStateManagerConfig supervisorStateManagerConfig
  )
  {
    super(
        id,
        Configs.valueOrDefault(
            ingestionSchema,
            new KafkaSupervisorIngestionSpec(dataSchema, ioConfig, tuningConfig)
        ),
        context,
        suspended,
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        kafkaIndexTaskClientFactory,
        mapper,
        emitter,
        monitorSchedulerConfig,
        rowIngestionMetersFactory,
        supervisorStateManagerConfig,
        usePerpetuallyRunningTasks
    );
  }

  @Override
  public String getType()
  {
    return TASK_TYPE;
  }

  @Nonnull
  @JsonIgnore
  @Override
  public Set<ResourceAction> getInputSourceResources()
  {
    return KafkaIndexTask.INPUT_SOURCE_RESOURCES;
  }

  @Override
  public String getSource()
  {
    return getIoConfig() != null ? getIoConfig().getStream() : null;
  }

  @Override
  public Supervisor createSupervisor()
  {
    return new KafkaSupervisor(
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        (KafkaIndexTaskClientFactory) indexTaskClientFactory,
        mapper,
        this,
        rowIngestionMetersFactory
    );
  }

  @Override
  @Deprecated
  @JsonProperty
  public KafkaSupervisorTuningConfig getTuningConfig()
  {
    return (KafkaSupervisorTuningConfig) super.getTuningConfig();
  }

  @Override
  @Deprecated
  @JsonProperty
  public KafkaSupervisorIOConfig getIoConfig()
  {
    return (KafkaSupervisorIOConfig) super.getIoConfig();
  }

  @Override
  @JsonProperty
  public KafkaSupervisorIngestionSpec getSpec()
  {
    return (KafkaSupervisorIngestionSpec) super.getSpec();
  }

  @Override
  protected KafkaSupervisorSpec toggleSuspend(boolean suspend)
  {
    return new KafkaSupervisorSpec(
        getId(),
        getSpec(),
        getDataSchema(),
        getTuningConfig(),
        getIoConfig(),
        getContext(),
        suspend,
        usePerpetuallyRunningTasks(),
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        (KafkaIndexTaskClientFactory) indexTaskClientFactory,
        mapper,
        emitter,
        monitorSchedulerConfig,
        rowIngestionMetersFactory,
        supervisorStateManagerConfig
    );
  }

  /**
   * Extends {@link SeekableStreamSupervisorSpec#validateSpecUpdateTo} to ensure that the proposed spec and current spec are either both multi-topic or both single-topic.
   * <p>
   * getSource() returns the same string (exampleTopic) for "topicPattern=exampleTopic" and "topic=exampleTopic".
   * This override prevents this case from being considered a valid update.
   * </p>
   * @param proposedSpec the proposed supervisor spec
   * @throws DruidException if the proposed spec is not a Kafka spec or if the proposed spec changes from multi-topic to single-topic or vice versa
   */
  @Override
  public void validateSpecUpdateTo(SupervisorSpec proposedSpec) throws DruidException
  {
    if (!(proposedSpec instanceof KafkaSupervisorSpec)) {
      throw InvalidInput.exception(
          "Cannot change spec from type[%s] to type[%s]", getClass().getSimpleName(), proposedSpec.getClass().getSimpleName()
      );
    }
    KafkaSupervisorSpec other = (KafkaSupervisorSpec) proposedSpec;
    if (this.getSpec().getIOConfig().isMultiTopic() != other.getSpec().getIOConfig().isMultiTopic()) {
      throw InvalidInput.exception(
          SeekableStreamSupervisorSpec.ILLEGAL_INPUT_SOURCE_UPDATE_ERROR_MESSAGE,
          StringUtils.format("(%s) %s", this.getSpec().getIOConfig().isMultiTopic() ? "multi-topic" : "single-topic", this.getSource()),
          StringUtils.format("(%s) %s", other.getSpec().getIOConfig().isMultiTopic() ? "multi-topic" : "single-topic", other.getSource())
      );
    }

    super.validateSpecUpdateTo(proposedSpec);
  }

  @Override
  public String toString()
  {
    return "KafkaSupervisorSpec{" +
           "dataSchema=" + getDataSchema() +
           ", tuningConfig=" + getTuningConfig() +
           ", ioConfig=" + getIoConfig() +
           ", context=" + getContext() +
           ", suspend=" + isSuspended() +
           '}';
  }
}
