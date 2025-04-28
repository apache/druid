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
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.indexing.kafka.KafkaIndexTaskClientFactory;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.supervisor.Supervisor;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManagerConfig;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorSpec;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.metrics.DruidMonitorSchedulerConfig;
import org.apache.druid.segment.incremental.RowIngestionMetersFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class KafkaSupervisorSpec extends SeekableStreamSupervisorSpec
{
  static final String TASK_TYPE = "kafka";

  @JsonCreator
  public KafkaSupervisorSpec(
      @JsonProperty("spec") @Nullable KafkaSupervisorIngestionSpec ingestionSchema,
      @JsonProperty("dataSchema") @Nullable DataSchema dataSchema,
      @JsonProperty("tuningConfig") @Nullable KafkaSupervisorTuningConfig tuningConfig,
      @JsonProperty("ioConfig") @Nullable KafkaSupervisorIOConfig ioConfig,
      @JsonProperty("context") Map<String, Object> context,
      @JsonProperty("suspended") Boolean suspended,
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
        ingestionSchema != null
        ? ingestionSchema
        : new KafkaSupervisorIngestionSpec(
            dataSchema,
            ioConfig,
            tuningConfig != null
            ? tuningConfig
            : KafkaSupervisorTuningConfig.defaultConfig()
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
        supervisorStateManagerConfig
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
    return Collections.singleton(new ResourceAction(
        new Resource(TASK_TYPE, ResourceType.EXTERNAL),
        Action.READ
    ));
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
        getSpec(),
        getDataSchema(),
        getTuningConfig(),
        getIoConfig(),
        getContext(),
        suspend,
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

  @Override
  public void validateProposedSpecEvolution(SupervisorSpec that) throws IllegalArgumentException
  {
    if (!(that instanceof KafkaSupervisorSpec)) {
      throw new IllegalArgumentException("Cannot evolve to " + that.getType() + " from " + getType());
    }
    KafkaSupervisorSpec other = (KafkaSupervisorSpec) that;
    if (this.getSource() == null || other.getSource() == null) {
      // I don't think this is possible, but covering just in case.
      throw new IllegalArgumentException(
          "Cannot consider KafkaSupervisorSpec evolution when one or both of the specs have not provided either a "
          + "topic OR topicPattern");
    }

    // Future enhancements could allow for topicPattern to be changed in a way where the new source is additive to the
    // old source. If we did that now, there would be metadata issues due to {@link KafkaDataSourceMetadata}
    // implementation details that aren't set up to handle evolution of metadata in this way.
    if (!this.getSource().equals(other.getSource())) {
      throw new IllegalArgumentException(
          "Your proposed KafkaSupervisorSpec evolution is invalid. You are attempting to change the topic/topicPattern "
          + "from " + this.getSource() + " to " + other.getSource() + ". This is not supported. If you "
          + "want to change the topic or topicPattern for a supervisor, you must first terminate the supervisor. "
          + "Then create a new one in suspended state with the new topic or topicPattern. Lastly, you will have to "
          + "reset the supervisor offsets. Finally, you can resume the new supervisor. Note that doing this reset can "
          + "cause duplicate events or lost events if any topics who were in the previous supervisor remain in the new "
          + "one.");
    }
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
