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

package org.apache.druid.server.coordinator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.common.config.JacksonConfigManager;
import org.apache.druid.java.util.common.IAE;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class is for users to change their configurations while their Druid cluster is running.
 * These configurations are designed to allow only simple values rather than complicated JSON objects.
 *
 * @see JacksonConfigManager
 * @see org.apache.druid.common.config.ConfigManager
 */
public class CoordinatorDynamicConfig
{
  public static final String CONFIG_KEY = "coordinator.config";

  private final long millisToWaitBeforeDeleting;
  private final long mergeBytesLimit;
  private final int mergeSegmentsLimit;
  private final int maxSegmentsToMove;
  private final int replicantLifetime;
  private final int replicationThrottleLimit;
  private final int balancerComputeThreads;
  private final boolean emitBalancingStats;
  private final boolean killAllDataSources;
  private final Set<String> killableDataSources;
  private final Set<String> historicalNodesInMaintenance;
  private final int nodesInMaintenancePriority;

  // The pending segments of the dataSources in this list are not killed.
  private final Set<String> protectedPendingSegmentDatasources;

  /**
   * The maximum number of segments that could be queued for loading to any given server.
   * Default values is 0 with the meaning of "unbounded" (any number of
   * segments could be added to the loading queue for any server).
   * See {@link LoadQueuePeon}, {@link org.apache.druid.server.coordinator.rules.LoadRule#run}
   */
  private final int maxSegmentsInNodeLoadingQueue;

  @JsonCreator
  public CoordinatorDynamicConfig(
      @JsonProperty("millisToWaitBeforeDeleting") long millisToWaitBeforeDeleting,
      @JsonProperty("mergeBytesLimit") long mergeBytesLimit,
      @JsonProperty("mergeSegmentsLimit") int mergeSegmentsLimit,
      @JsonProperty("maxSegmentsToMove") int maxSegmentsToMove,
      @JsonProperty("replicantLifetime") int replicantLifetime,
      @JsonProperty("replicationThrottleLimit") int replicationThrottleLimit,
      @JsonProperty("balancerComputeThreads") int balancerComputeThreads,
      @JsonProperty("emitBalancingStats") boolean emitBalancingStats,

      // Type is Object here so that we can support both string and list as
      // coordinator console can not send array of strings in the update request.
      // See https://github.com/apache/incubator-druid/issues/3055
      @JsonProperty("killDataSourceWhitelist") Object killableDataSources,
      @JsonProperty("killAllDataSources") boolean killAllDataSources,
      @JsonProperty("killPendingSegmentsSkipList") Object protectedPendingSegmentDatasources,
      @JsonProperty("maxSegmentsInNodeLoadingQueue") int maxSegmentsInNodeLoadingQueue,
      @JsonProperty("historicalNodesInMaintenance") Object historicalNodesInMaintenance,
      @JsonProperty("nodesInMaintenancePriority") int nodesInMaintenancePriority
  )
  {
    this.millisToWaitBeforeDeleting = millisToWaitBeforeDeleting;
    this.mergeBytesLimit = mergeBytesLimit;
    this.mergeSegmentsLimit = mergeSegmentsLimit;
    this.maxSegmentsToMove = maxSegmentsToMove;
    this.replicantLifetime = replicantLifetime;
    this.replicationThrottleLimit = replicationThrottleLimit;
    this.balancerComputeThreads = Math.max(balancerComputeThreads, 1);
    this.emitBalancingStats = emitBalancingStats;
    this.killAllDataSources = killAllDataSources;
    this.killableDataSources = parseJsonStringOrArray(killableDataSources);
    this.protectedPendingSegmentDatasources = parseJsonStringOrArray(protectedPendingSegmentDatasources);
    this.maxSegmentsInNodeLoadingQueue = maxSegmentsInNodeLoadingQueue;
    this.historicalNodesInMaintenance = parseJsonStringOrArray(historicalNodesInMaintenance);
    Preconditions.checkArgument(
        nodesInMaintenancePriority >= 0 && nodesInMaintenancePriority <= 10,
        "nodesInMaintenancePriority should be in range [0, 10]"
    );
    this.nodesInMaintenancePriority = nodesInMaintenancePriority;

    if (this.killAllDataSources && !this.killableDataSources.isEmpty()) {
      throw new IAE("can't have killAllDataSources and non-empty killDataSourceWhitelist");
    }
  }

  private static Set<String> parseJsonStringOrArray(Object jsonStringOrArray)
  {
    if (jsonStringOrArray instanceof String) {
      String[] list = ((String) jsonStringOrArray).split(",");
      Set<String> result = new HashSet<>();
      for (String item : list) {
        String trimmed = item.trim();
        if (!trimmed.isEmpty()) {
          result.add(trimmed);
        }
      }
      return result;
    } else if (jsonStringOrArray instanceof Collection) {
      return ImmutableSet.copyOf(((Collection) jsonStringOrArray));
    } else {
      return ImmutableSet.of();
    }
  }

  public static AtomicReference<CoordinatorDynamicConfig> watch(final JacksonConfigManager configManager)
  {
    return configManager.watch(
        CoordinatorDynamicConfig.CONFIG_KEY,
        CoordinatorDynamicConfig.class,
        CoordinatorDynamicConfig.builder().build()
    );
  }

  @Nonnull
  public static CoordinatorDynamicConfig current(final JacksonConfigManager configManager)
  {
    return Preconditions.checkNotNull(watch(configManager).get(), "Got null config from watcher?!");
  }

  @JsonProperty
  public long getMillisToWaitBeforeDeleting()
  {
    return millisToWaitBeforeDeleting;
  }

  @JsonProperty
  public long getMergeBytesLimit()
  {
    return mergeBytesLimit;
  }

  @JsonProperty
  public boolean emitBalancingStats()
  {
    return emitBalancingStats;
  }

  @JsonProperty
  public int getMergeSegmentsLimit()
  {
    return mergeSegmentsLimit;
  }

  @JsonProperty
  public int getMaxSegmentsToMove()
  {
    return maxSegmentsToMove;
  }

  @JsonProperty
  public int getReplicantLifetime()
  {
    return replicantLifetime;
  }

  @JsonProperty
  public int getReplicationThrottleLimit()
  {
    return replicationThrottleLimit;
  }

  @JsonProperty
  public int getBalancerComputeThreads()
  {
    return balancerComputeThreads;
  }

  /**
   * List of dataSources for which kill tasks are sent in
   * {@link org.apache.druid.server.coordinator.helper.DruidCoordinatorSegmentKiller}.
   */
  @JsonProperty("killDataSourceWhitelist")
  public Set<String> getKillableDataSources()
  {
    return killableDataSources;
  }

  @JsonProperty
  public boolean isKillAllDataSources()
  {
    return killAllDataSources;
  }

  /**
   * List of dataSources for which pendingSegments are NOT cleaned up
   * in {@link DruidCoordinatorCleanupPendingSegments}.
   */
  @JsonProperty
  public Set<String> getProtectedPendingSegmentDatasources()
  {
    return protectedPendingSegmentDatasources;
  }

  @JsonProperty
  public int getMaxSegmentsInNodeLoadingQueue()
  {
    return maxSegmentsInNodeLoadingQueue;
  }

  /**
   * Historical nodes list in maintenance mode. Coordinator doesn't assign new segments on those nodes and moves
   * segments from those nodes according to a specified priority.
   *
   * @return list of host:port entries
   */
  @JsonProperty
  public Set<String> getHistoricalNodesInMaintenance()
  {
    return historicalNodesInMaintenance;
  }

  /**
   * Priority of segments from servers in maintenance. Coordinator takes ceil(maxSegmentsToMove * (priority / 10))
   * from servers in maitenance during balancing phase, i.e.:
   * 0 - no segments from servers in maintenance will be processed during balancing
   * 5 - 50% segments from servers in maintenance
   * 10 - 100% segments from servers in maintenance
   * By leveraging the priority an operator can prevent general nodes from overload or decrease maitenance time
   * instead.
   *
   * @return number in range [0, 10]
   */
  @JsonProperty
  public int getNodesInMaintenancePriority()
  {
    return nodesInMaintenancePriority;
  }

  @Override
  public String toString()
  {
    return "CoordinatorDynamicConfig{" +
           "millisToWaitBeforeDeleting=" + millisToWaitBeforeDeleting +
           ", mergeBytesLimit=" + mergeBytesLimit +
           ", mergeSegmentsLimit=" + mergeSegmentsLimit +
           ", maxSegmentsToMove=" + maxSegmentsToMove +
           ", replicantLifetime=" + replicantLifetime +
           ", replicationThrottleLimit=" + replicationThrottleLimit +
           ", balancerComputeThreads=" + balancerComputeThreads +
           ", emitBalancingStats=" + emitBalancingStats +
           ", killAllDataSources=" + killAllDataSources +
           ", killDataSourceWhitelist=" + killableDataSources +
           ", protectedPendingSegmentDatasources=" + protectedPendingSegmentDatasources +
           ", maxSegmentsInNodeLoadingQueue=" + maxSegmentsInNodeLoadingQueue +
           ", historicalNodesInMaintenance=" + historicalNodesInMaintenance +
           ", nodesInMaintenancePriority=" + nodesInMaintenancePriority +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    CoordinatorDynamicConfig that = (CoordinatorDynamicConfig) o;

    if (millisToWaitBeforeDeleting != that.millisToWaitBeforeDeleting) {
      return false;
    }
    if (mergeBytesLimit != that.mergeBytesLimit) {
      return false;
    }
    if (mergeSegmentsLimit != that.mergeSegmentsLimit) {
      return false;
    }
    if (maxSegmentsToMove != that.maxSegmentsToMove) {
      return false;
    }
    if (replicantLifetime != that.replicantLifetime) {
      return false;
    }
    if (replicationThrottleLimit != that.replicationThrottleLimit) {
      return false;
    }
    if (balancerComputeThreads != that.balancerComputeThreads) {
      return false;
    }
    if (emitBalancingStats != that.emitBalancingStats) {
      return false;
    }
    if (killAllDataSources != that.killAllDataSources) {
      return false;
    }
    if (maxSegmentsInNodeLoadingQueue != that.maxSegmentsInNodeLoadingQueue) {
      return false;
    }
    if (!Objects.equals(killableDataSources, that.killableDataSources)) {
      return false;
    }
    if (!Objects.equals(protectedPendingSegmentDatasources, that.protectedPendingSegmentDatasources)) {
      return false;
    }
    if (!Objects.equals(historicalNodesInMaintenance, that.historicalNodesInMaintenance)) {
      return false;
    }
    return nodesInMaintenancePriority == that.nodesInMaintenancePriority;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        millisToWaitBeforeDeleting,
        mergeBytesLimit,
        mergeSegmentsLimit,
        maxSegmentsToMove,
        replicantLifetime,
        replicationThrottleLimit,
        balancerComputeThreads,
        emitBalancingStats,
        killAllDataSources,
        maxSegmentsInNodeLoadingQueue,
        killableDataSources,
        protectedPendingSegmentDatasources,
        historicalNodesInMaintenance,
        nodesInMaintenancePriority
    );
  }

  public static Builder builder()
  {
    return new Builder();
  }

  public static class Builder
  {
    private static final long DEFAULT_MILLIS_TO_WAIT_BEFORE_DELETING = TimeUnit.MINUTES.toMillis(15);
    private static final long DEFAULT_MERGE_BYTES_LIMIT = 524288000L;
    private static final int DEFAULT_MERGE_SEGMENTS_LIMIT = 100;
    private static final int DEFAULT_MAX_SEGMENTS_TO_MOVE = 5;
    private static final int DEFAULT_REPLICANT_LIFETIME = 15;
    private static final int DEFAULT_REPLICATION_THROTTLE_LIMIT = 10;
    private static final int DEFAULT_BALANCER_COMPUTE_THREADS = 1;
    private static final boolean DEFAULT_EMIT_BALANCING_STATS = false;
    private static final boolean DEFAULT_KILL_ALL_DATA_SOURCES = false;
    private static final int DEFAULT_MAX_SEGMENTS_IN_NODE_LOADING_QUEUE = 0;
    private static final int DEFAULT_MAINTENANCE_MODE_SEGMENTS_PRIORITY = 7;

    private Long millisToWaitBeforeDeleting;
    private Long mergeBytesLimit;
    private Integer mergeSegmentsLimit;
    private Integer maxSegmentsToMove;
    private Integer replicantLifetime;
    private Integer replicationThrottleLimit;
    private Boolean emitBalancingStats;
    private Integer balancerComputeThreads;
    private Object killableDataSources;
    private Boolean killAllDataSources;
    private Object killPendingSegmentsSkipList;
    private Integer maxSegmentsInNodeLoadingQueue;
    private Object maintenanceList;
    private Integer maintenanceModeSegmentsPriority;

    public Builder()
    {
    }

    @JsonCreator
    public Builder(
        @JsonProperty("millisToWaitBeforeDeleting") @Nullable Long millisToWaitBeforeDeleting,
        @JsonProperty("mergeBytesLimit") @Nullable Long mergeBytesLimit,
        @JsonProperty("mergeSegmentsLimit") @Nullable Integer mergeSegmentsLimit,
        @JsonProperty("maxSegmentsToMove") @Nullable Integer maxSegmentsToMove,
        @JsonProperty("replicantLifetime") @Nullable Integer replicantLifetime,
        @JsonProperty("replicationThrottleLimit") @Nullable Integer replicationThrottleLimit,
        @JsonProperty("balancerComputeThreads") @Nullable Integer balancerComputeThreads,
        @JsonProperty("emitBalancingStats") @Nullable Boolean emitBalancingStats,
        @JsonProperty("killDataSourceWhitelist") @Nullable Object killableDataSources,
        @JsonProperty("killAllDataSources") @Nullable Boolean killAllDataSources,
        @JsonProperty("killPendingSegmentsSkipList") @Nullable Object killPendingSegmentsSkipList,
        @JsonProperty("maxSegmentsInNodeLoadingQueue") @Nullable Integer maxSegmentsInNodeLoadingQueue,
        @JsonProperty("historicalNodesInMaintenance") @Nullable Object maintenanceList,
        @JsonProperty("nodesInMaintenancePriority") @Nullable Integer maintenanceModeSegmentsPriority
    )
    {
      this.millisToWaitBeforeDeleting = millisToWaitBeforeDeleting;
      this.mergeBytesLimit = mergeBytesLimit;
      this.mergeSegmentsLimit = mergeSegmentsLimit;
      this.maxSegmentsToMove = maxSegmentsToMove;
      this.replicantLifetime = replicantLifetime;
      this.replicationThrottleLimit = replicationThrottleLimit;
      this.balancerComputeThreads = balancerComputeThreads;
      this.emitBalancingStats = emitBalancingStats;
      this.killAllDataSources = killAllDataSources;
      this.killableDataSources = killableDataSources;
      this.killPendingSegmentsSkipList = killPendingSegmentsSkipList;
      this.maxSegmentsInNodeLoadingQueue = maxSegmentsInNodeLoadingQueue;
      this.maintenanceList = maintenanceList;
      this.maintenanceModeSegmentsPriority = maintenanceModeSegmentsPriority;
    }

    public Builder withMillisToWaitBeforeDeleting(long millisToWaitBeforeDeleting)
    {
      this.millisToWaitBeforeDeleting = millisToWaitBeforeDeleting;
      return this;
    }

    public Builder withMergeBytesLimit(long mergeBytesLimit)
    {
      this.mergeBytesLimit = mergeBytesLimit;
      return this;
    }

    public Builder withMergeSegmentsLimit(int mergeSegmentsLimit)
    {
      this.mergeSegmentsLimit = mergeSegmentsLimit;
      return this;
    }

    public Builder withMaxSegmentsToMove(int maxSegmentsToMove)
    {
      this.maxSegmentsToMove = maxSegmentsToMove;
      return this;
    }

    public Builder withReplicantLifetime(int replicantLifetime)
    {
      this.replicantLifetime = replicantLifetime;
      return this;
    }

    public Builder withReplicationThrottleLimit(int replicationThrottleLimit)
    {
      this.replicationThrottleLimit = replicationThrottleLimit;
      return this;
    }

    public Builder withBalancerComputeThreads(int balancerComputeThreads)
    {
      this.balancerComputeThreads = balancerComputeThreads;
      return this;
    }

    public Builder withEmitBalancingStats(boolean emitBalancingStats)
    {
      this.emitBalancingStats = emitBalancingStats;
      return this;
    }

    public Builder withKillDataSourceWhitelist(Set<String> killDataSourceWhitelist)
    {
      this.killableDataSources = killDataSourceWhitelist;
      return this;
    }

    public Builder withKillAllDataSources(boolean killAllDataSources)
    {
      this.killAllDataSources = killAllDataSources;
      return this;
    }

    public Builder withMaxSegmentsInNodeLoadingQueue(int maxSegmentsInNodeLoadingQueue)
    {
      this.maxSegmentsInNodeLoadingQueue = maxSegmentsInNodeLoadingQueue;
      return this;
    }

    public Builder withMaintenanceList(Set<String> list)
    {
      this.maintenanceList = list;
      return this;
    }

    public Builder withMaintenanceModeSegmentsPriority(Integer priority)
    {
      this.maintenanceModeSegmentsPriority = priority;
      return this;
    }

    public CoordinatorDynamicConfig build()
    {
      return new CoordinatorDynamicConfig(
          millisToWaitBeforeDeleting == null ? DEFAULT_MILLIS_TO_WAIT_BEFORE_DELETING : millisToWaitBeforeDeleting,
          mergeBytesLimit == null ? DEFAULT_MERGE_BYTES_LIMIT : mergeBytesLimit,
          mergeSegmentsLimit == null ? DEFAULT_MERGE_SEGMENTS_LIMIT : mergeSegmentsLimit,
          maxSegmentsToMove == null ? DEFAULT_MAX_SEGMENTS_TO_MOVE : maxSegmentsToMove,
          replicantLifetime == null ? DEFAULT_REPLICANT_LIFETIME : replicantLifetime,
          replicationThrottleLimit == null ? DEFAULT_REPLICATION_THROTTLE_LIMIT : replicationThrottleLimit,
          balancerComputeThreads == null ? DEFAULT_BALANCER_COMPUTE_THREADS : balancerComputeThreads,
          emitBalancingStats == null ? DEFAULT_EMIT_BALANCING_STATS : emitBalancingStats,
          killableDataSources,
          killAllDataSources == null ? DEFAULT_KILL_ALL_DATA_SOURCES : killAllDataSources,
          killPendingSegmentsSkipList,
          maxSegmentsInNodeLoadingQueue == null
          ? DEFAULT_MAX_SEGMENTS_IN_NODE_LOADING_QUEUE
          : maxSegmentsInNodeLoadingQueue,
          maintenanceList,
          maintenanceModeSegmentsPriority == null
          ? DEFAULT_MAINTENANCE_MODE_SEGMENTS_PRIORITY
          : maintenanceModeSegmentsPriority
      );
    }

    public CoordinatorDynamicConfig build(CoordinatorDynamicConfig defaults)
    {
      return new CoordinatorDynamicConfig(
          millisToWaitBeforeDeleting == null ? defaults.getMillisToWaitBeforeDeleting() : millisToWaitBeforeDeleting,
          mergeBytesLimit == null ? defaults.getMergeBytesLimit() : mergeBytesLimit,
          mergeSegmentsLimit == null ? defaults.getMergeSegmentsLimit() : mergeSegmentsLimit,
          maxSegmentsToMove == null ? defaults.getMaxSegmentsToMove() : maxSegmentsToMove,
          replicantLifetime == null ? defaults.getReplicantLifetime() : replicantLifetime,
          replicationThrottleLimit == null ? defaults.getReplicationThrottleLimit() : replicationThrottleLimit,
          balancerComputeThreads == null ? defaults.getBalancerComputeThreads() : balancerComputeThreads,
          emitBalancingStats == null ? defaults.emitBalancingStats() : emitBalancingStats,
          killableDataSources == null ? defaults.getKillableDataSources() : killableDataSources,
          killAllDataSources == null ? defaults.isKillAllDataSources() : killAllDataSources,
          killPendingSegmentsSkipList == null
          ? defaults.getProtectedPendingSegmentDatasources()
          : killPendingSegmentsSkipList,
          maxSegmentsInNodeLoadingQueue == null
          ? defaults.getMaxSegmentsInNodeLoadingQueue()
          : maxSegmentsInNodeLoadingQueue,
          maintenanceList == null ? defaults.getHistoricalNodesInMaintenance() : maintenanceList,
          maintenanceModeSegmentsPriority == null
          ? defaults.getNodesInMaintenancePriority()
          : maintenanceModeSegmentsPriority
      );
    }
  }
}
