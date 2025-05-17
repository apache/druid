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

package org.apache.druid.testing.cluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.common.config.Configs;
import org.apache.druid.indexing.common.task.Task;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * Config passed in the context parameter {@code "clusterTesting"} of a task,
 * used for testing scalability of a Druid cluster by introducing faults at
 * various interface points.
 */
public class ClusterTestingTaskConfig
{
  /**
   * Default config which does not trigger any faults for this task.
   */
  public static final ClusterTestingTaskConfig DEFAULT = new ClusterTestingTaskConfig(null, null, null, null);

  private final MetadataConfig metadataConfig;
  private final OverlordClientConfig overlordClientConfig;
  private final CoordinatorClientConfig coordinatorClientConfig;
  private final TaskActionClientConfig taskActionClientConfig;

  @JsonCreator
  public ClusterTestingTaskConfig(
      @JsonProperty("metadataConfig") @Nullable MetadataConfig metadataConfig,
      @JsonProperty("overlordClientConfig") @Nullable OverlordClientConfig overlordClientConfig,
      @JsonProperty("coordinatorClientConfig") @Nullable CoordinatorClientConfig coordinatorClientConfig,
      @JsonProperty("taskActionClientConfig") @Nullable TaskActionClientConfig taskActionClientConfig
  )
  {
    this.metadataConfig = Configs.valueOrDefault(metadataConfig, MetadataConfig.DEFAULT);
    this.overlordClientConfig = Configs.valueOrDefault(overlordClientConfig, OverlordClientConfig.DEFAULT);
    this.coordinatorClientConfig = Configs.valueOrDefault(coordinatorClientConfig, CoordinatorClientConfig.DEFAULT);
    this.taskActionClientConfig = Configs.valueOrDefault(taskActionClientConfig, TaskActionClientConfig.DEFAULT);
  }

  /**
   * Creates a {@link ClusterTestingTaskConfig} for the given Task by reading
   * the task context parameter {@code "clusterTesting"}.
   *
   * @return Default config {@link ClusterTestingTaskConfig#DEFAULT} if not
   * specified in the task context parameters.
   */
  public static ClusterTestingTaskConfig forTask(Task task, ObjectMapper mapper) throws JsonProcessingException
  {
    final Map<String, Object> configAsMap = task.getContextValue("clusterTesting");
    final String json = mapper.writeValueAsString(configAsMap);

    final ClusterTestingTaskConfig config = mapper.readValue(json, ClusterTestingTaskConfig.class);
    return Configs.valueOrDefault(config, ClusterTestingTaskConfig.DEFAULT);
  }

  public MetadataConfig getMetadataConfig()
  {
    return metadataConfig;
  }

  public OverlordClientConfig getOverlordClientConfig()
  {
    return overlordClientConfig;
  }

  public CoordinatorClientConfig getCoordinatorClientConfig()
  {
    return coordinatorClientConfig;
  }

  public TaskActionClientConfig getTaskActionClientConfig()
  {
    return taskActionClientConfig;
  }

  @Override
  public String toString()
  {
    return '{' +
           "overlordClientConfig=" + overlordClientConfig +
           ", coordinatorClientConfig=" + coordinatorClientConfig +
           ", taskActionClientConfig=" + taskActionClientConfig +
           '}';
  }

  /**
   * Config for manipulating communication of the task with the Overlord.
   */
  public static class OverlordClientConfig
  {
    private static final OverlordClientConfig DEFAULT = new OverlordClientConfig();

    @Override
    public String toString()
    {
      return "";
    }
  }

  /**
   * Config for manipulating communication of the task with the Coordinator.
   */
  public static class CoordinatorClientConfig
  {
    private static final CoordinatorClientConfig DEFAULT = new CoordinatorClientConfig(null);

    private final Duration minSegmentHandoffDelay;

    @JsonCreator
    public CoordinatorClientConfig(
        @JsonProperty("minSegmentHandoffDelay") @Nullable Duration minSegmentHandoffDelay
    )
    {
      this.minSegmentHandoffDelay = minSegmentHandoffDelay;
    }

    /**
     * Minimum duration for which segment handoff is assumed to not have completed.
     * The actual handoff status is fetched from the Coordinator only after this
     * duration has elapsed. This delay applies to each segment separately.
     * <p>
     * For any segment,
     * <pre>
     * observed handoff time = actual handoff time + minSegmentHandoffDelay
     * </pre>
     */
    @Nullable
    public Duration getMinSegmentHandoffDelay()
    {
      return minSegmentHandoffDelay;
    }

    @Override
    public String toString()
    {
      return "{" +
             "minSegmentHandoffDelay=" + minSegmentHandoffDelay +
             '}';
    }
  }

  /**
   * Config for manipulating task actions submitted by the task to the Overlord.
   */
  public static class TaskActionClientConfig
  {
    private static final TaskActionClientConfig DEFAULT = new TaskActionClientConfig(null, null);

    private final Duration segmentPublishDelay;
    private final Duration segmentAllocateDelay;

    @JsonCreator
    public TaskActionClientConfig(
        @JsonProperty("segmentAllocateDelay") @Nullable Duration segmentAllocateDelay,
        @JsonProperty("segmentPublishDelay") @Nullable Duration segmentPublishDelay
    )
    {
      this.segmentAllocateDelay = segmentAllocateDelay;
      this.segmentPublishDelay = segmentPublishDelay;
    }

    /**
     * Duration to wait before sending a segment publish request to the Overlord.
     * <p>
     * For each publish request (containing one or more segments),
     * <pre>
     * observed publish time = actual publish time + segmentPublishDelay
     * </pre>
     */
    @Nullable
    public Duration getSegmentPublishDelay()
    {
      return segmentPublishDelay;
    }

    /**
     * Duration to wait before sending a segment allocate request to the Overlord.
     * <p>
     * For each segment,
     * <pre>
     * observed segment allocation time = actual allocation time + segmentAllocateDelay
     * </pre>
     */
    @Nullable
    public Duration getSegmentAllocateDelay()
    {
      return segmentAllocateDelay;
    }

    @Override
    public String toString()
    {
      return "{" +
             "segmentPublishDelay=" + segmentPublishDelay +
             ", segmentAllocateDelay=" + segmentAllocateDelay +
             '}';
    }
  }

  /**
   * Config for manipulating metadata operations performed by the Overlord for
   * the task.
   */
  public static class MetadataConfig
  {
    private static final MetadataConfig DEFAULT = new MetadataConfig(null);

    private final boolean cleanupPendingSegments;

    @JsonCreator
    public MetadataConfig(
        @JsonProperty("cleanupPendingSegments") @Nullable Boolean cleanupPendingSegments
    )
    {
      this.cleanupPendingSegments = Configs.valueOrDefault(cleanupPendingSegments, true);
    }

    public boolean isCleanupPendingSegments()
    {
      return cleanupPendingSegments;
    }
  }
}
