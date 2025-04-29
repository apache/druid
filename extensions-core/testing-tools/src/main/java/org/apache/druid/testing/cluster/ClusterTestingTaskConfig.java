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
import org.apache.druid.common.config.Configs;
import org.joda.time.Duration;

import javax.annotation.Nullable;

/**
 * Config used for testing scalability of a Druid cluster by introducing faults
 * at various interface points.
 */
public class ClusterTestingTaskConfig
{
  private final OverlordClientConfig overlordClientConfig;
  private final CoordinatorClientConfig coordinatorClientConfig;
  private final TaskActionClientConfig taskActionClientConfig;

  @JsonCreator
  public ClusterTestingTaskConfig(
      @JsonProperty("overlordClientConfig") @Nullable OverlordClientConfig overlordClientConfig,
      @JsonProperty("coordinatorClientConfig") @Nullable CoordinatorClientConfig coordinatorClientConfig,
      @JsonProperty("taskActionClientConfig") @Nullable TaskActionClientConfig taskActionClientConfig
  )
  {
    this.overlordClientConfig = Configs.valueOrDefault(overlordClientConfig, OverlordClientConfig.DEFAULT);
    this.coordinatorClientConfig = Configs.valueOrDefault(coordinatorClientConfig, CoordinatorClientConfig.DEFAULT);
    this.taskActionClientConfig = Configs.valueOrDefault(taskActionClientConfig, TaskActionClientConfig.DEFAULT);
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
   * Config for faulty overlord client.
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
   * Config for faulty coordinator client.
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
   * Config for faulty task action client.
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
}
