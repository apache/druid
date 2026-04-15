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

package org.apache.druid.indexing.kafka;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.task.AbstractTask;
import org.apache.druid.indexing.common.task.PendingSegmentAllocatingTask;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.ResourceAction;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Indexing task that consumes from a Kafka topic using share group semantics
 * (KIP-932). Unlike {@link KafkaIndexTask}, this task does not manage offsets
 * client-side. The Kafka broker tracks delivery state; the task explicitly
 * acknowledges records after segments are published.
 *
 * Phase 1: Single-threaded, no supervisor, no dedup cache.
 * The task polls, parses, builds segments, publishes, ACKs, and commits.
 */
public class ShareGroupIndexTask extends AbstractTask implements PendingSegmentAllocatingTask
{
  private static final Logger log = new Logger(ShareGroupIndexTask.class);
  private static final String TYPE = "index_kafka_share_group";

  public static final Set<ResourceAction> INPUT_SOURCE_RESOURCES = Set.of(
      AuthorizationUtils.createExternalResourceReadAction(KafkaIndexTaskModule.SCHEME)
  );

  private final DataSchema dataSchema;
  private final KafkaIndexTaskTuningConfig tuningConfig;
  private final ShareGroupIndexTaskIOConfig ioConfig;
  private final ObjectMapper configMapper;

  private final AtomicBoolean stopRequested = new AtomicBoolean(false);

  @JsonCreator
  public ShareGroupIndexTask(
      @JsonProperty("id") @Nullable String id,
      @JsonProperty("resource") @Nullable TaskResource taskResource,
      @JsonProperty("dataSchema") DataSchema dataSchema,
      @JsonProperty("tuningConfig") KafkaIndexTaskTuningConfig tuningConfig,
      @JsonProperty("ioConfig") ShareGroupIndexTaskIOConfig ioConfig,
      @JsonProperty("context") @Nullable Map<String, Object> context,
      @JacksonInject ObjectMapper configMapper
  )
  {
    super(
        getOrMakeId(id, TYPE, dataSchema.getDataSource()),
        null,
        taskResource,
        dataSchema.getDataSource(),
        addConcurrentLocksContext(context),
        IngestionMode.APPEND
    );
    this.dataSchema = dataSchema;
    this.tuningConfig = tuningConfig;
    this.ioConfig = ioConfig;
    this.configMapper = configMapper;
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient)
  {
    return true;
  }

  @Nonnull
  @JsonIgnore
  @Override
  public Set<ResourceAction> getInputSourceResources()
  {
    return INPUT_SOURCE_RESOURCES;
  }

  @Override
  public TaskStatus runTask(TaskToolbox toolbox) throws Exception
  {
    return new ShareGroupIndexTaskRunner(this, toolbox, configMapper).run();
  }

  @Override
  public void stopGracefully(TaskConfig taskConfig)
  {
    log.info("Graceful stop requested for task[%s].", getId());
    stopRequested.set(true);
  }

  @JsonProperty
  public DataSchema getDataSchema()
  {
    return dataSchema;
  }

  @JsonProperty
  public KafkaIndexTaskTuningConfig getTuningConfig()
  {
    return tuningConfig;
  }

  @JsonProperty("ioConfig")
  public ShareGroupIndexTaskIOConfig getIOConfig()
  {
    return ioConfig;
  }

  @Override
  public String getTaskAllocatorId()
  {
    return getTaskResource().getAvailabilityGroup();
  }

  boolean isStopRequested()
  {
    return stopRequested.get();
  }

  ObjectMapper getConfigMapper()
  {
    return configMapper;
  }

  private static Map<String, Object> addConcurrentLocksContext(@Nullable Map<String, Object> context)
  {
    final Map<String, Object> merged = new HashMap<>();
    if (context != null) {
      merged.putAll(context);
    }
    merged.putIfAbsent(Tasks.USE_CONCURRENT_LOCKS, true);
    return merged;
  }
}
