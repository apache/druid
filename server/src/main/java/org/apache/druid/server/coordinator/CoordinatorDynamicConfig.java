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
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
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

  private final long leadingTimeMillisBeforeCanMarkAsUnusedOvershadowedSegments;
  private final long mergeBytesLimit;
  private final int mergeSegmentsLimit;
  private final int maxSegmentsToMove;
  private final int replicantLifetime;
  private final int replicationThrottleLimit;
  private final int balancerComputeThreads;
  private final boolean emitBalancingStats;

  /**
   * If true, {@link org.apache.druid.server.coordinator.helper.DruidCoordinatorSegmentKiller} sends kill tasks for
   * unused segments in all data sources.
   */
  private final boolean killUnusedSegmentsInAllDataSources;

  /**
   * List of specific data sources for which kill tasks are sent in {@link
   * org.apache.druid.server.coordinator.helper.DruidCoordinatorSegmentKiller}.
   */
  private final Set<String> specificDataSourcesToKillUnusedSegmentsIn;
  private final Set<String> decommissioningNodes;
  private final int decommissioningMaxPercentOfMaxSegmentsToMove;

  /**
   * Stale pending segments belonging to the data sources in this list are not killed by {@link
   * DruidCoordinatorCleanupPendingSegments}. In other words, segments in these data sources are "protected".
   *
   * Pending segments are considered "stale" when their created_time is older than {@link
   * DruidCoordinatorCleanupPendingSegments#KEEP_PENDING_SEGMENTS_OFFSET} from now.
   */
  private final Set<String> dataSourcesToNotKillStalePendingSegmentsIn;

  /**
   * The maximum number of segments that could be queued for loading to any given server.
   * Default values is 0 with the meaning of "unbounded" (any number of
   * segments could be added to the loading queue for any server).
   * See {@link LoadQueuePeon}, {@link org.apache.druid.server.coordinator.rules.LoadRule#run}
   */
  private final int maxSegmentsInNodeLoadingQueue;

  @JsonCreator
  public CoordinatorDynamicConfig(
      // Keeping the legacy 'millisToWaitBeforeDeleting' property name for backward compatibility. When the project is
      // updated to Jackson 2.9 it could be changed, see https://github.com/apache/incubator-druid/issues/7152
      @JsonProperty("millisToWaitBeforeDeleting")
          long leadingTimeMillisBeforeCanMarkAsUnusedOvershadowedSegments,
      @JsonProperty("mergeBytesLimit") long mergeBytesLimit,
      @JsonProperty("mergeSegmentsLimit") int mergeSegmentsLimit,
      @JsonProperty("maxSegmentsToMove") int maxSegmentsToMove,
      @JsonProperty("replicantLifetime") int replicantLifetime,
      @JsonProperty("replicationThrottleLimit") int replicationThrottleLimit,
      @JsonProperty("balancerComputeThreads") int balancerComputeThreads,
      @JsonProperty("emitBalancingStats") boolean emitBalancingStats,
      // Type is Object here so that we can support both string and list as Coordinator console can not send array of
      // strings in the update request. See https://github.com/apache/incubator-druid/issues/3055.
      // Keeping the legacy 'killDataSourceWhitelist' property name for backward compatibility. When the project is
      // updated to Jackson 2.9 it could be changed, see https://github.com/apache/incubator-druid/issues/7152
      @JsonProperty("killDataSourceWhitelist") Object specificDataSourcesToKillUnusedSegmentsIn,
      // Keeping the legacy 'killAllDataSources' property name for backward compatibility. When the project is
      // updated to Jackson 2.9 it could be changed, see https://github.com/apache/incubator-druid/issues/7152
      @JsonProperty("killAllDataSources") boolean killUnusedSegmentsInAllDataSources,
      // Type is Object here so that we can support both string and list as Coordinator console can not send array of
      // strings in the update request, as well as for specificDataSourcesToKillUnusedSegmentsIn.
      // Keeping the legacy 'killPendingSegmentsSkipList' property name for backward compatibility. When the project is
      // updated to Jackson 2.9 it could be changed, see https://github.com/apache/incubator-druid/issues/7152
      @JsonProperty("killPendingSegmentsSkipList") Object dataSourcesToNotKillStalePendingSegmentsIn,
      @JsonProperty("maxSegmentsInNodeLoadingQueue") int maxSegmentsInNodeLoadingQueue,
      @JsonProperty("decommissioningNodes") Object decommissioningNodes,
      @JsonProperty("decommissioningMaxPercentOfMaxSegmentsToMove") int decommissioningMaxPercentOfMaxSegmentsToMove
  )
  {
    this.leadingTimeMillisBeforeCanMarkAsUnusedOvershadowedSegments =
        leadingTimeMillisBeforeCanMarkAsUnusedOvershadowedSegments;
    this.mergeBytesLimit = mergeBytesLimit;
    this.mergeSegmentsLimit = mergeSegmentsLimit;
    this.maxSegmentsToMove = maxSegmentsToMove;
    this.replicantLifetime = replicantLifetime;
    this.replicationThrottleLimit = replicationThrottleLimit;
    this.balancerComputeThreads = Math.max(balancerComputeThreads, 1);
    this.emitBalancingStats = emitBalancingStats;
    this.killUnusedSegmentsInAllDataSources = killUnusedSegmentsInAllDataSources;
    this.specificDataSourcesToKillUnusedSegmentsIn = parseJsonStringOrArray(specificDataSourcesToKillUnusedSegmentsIn);
    this.dataSourcesToNotKillStalePendingSegmentsIn =
        parseJsonStringOrArray(dataSourcesToNotKillStalePendingSegmentsIn);
    this.maxSegmentsInNodeLoadingQueue = maxSegmentsInNodeLoadingQueue;
    this.decommissioningNodes = parseJsonStringOrArray(decommissioningNodes);
    Preconditions.checkArgument(
        decommissioningMaxPercentOfMaxSegmentsToMove >= 0 && decommissioningMaxPercentOfMaxSegmentsToMove <= 100,
        "decommissioningMaxPercentOfMaxSegmentsToMove should be in range [0, 100]"
    );
    this.decommissioningMaxPercentOfMaxSegmentsToMove = decommissioningMaxPercentOfMaxSegmentsToMove;

    if (this.killUnusedSegmentsInAllDataSources && !this.specificDataSourcesToKillUnusedSegmentsIn.isEmpty()) {
      throw new IAE(
          "can't have killUnusedSegmentsInAllDataSources and non-empty specificDataSourcesToKillUnusedSegmentsIn"
      );
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

  @JsonProperty("millisToWaitBeforeDeleting")
  public long getLeadingTimeMillisBeforeCanMarkAsUnusedOvershadowedSegments()
  {
    return leadingTimeMillisBeforeCanMarkAsUnusedOvershadowedSegments;
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

  @JsonProperty("killDataSourceWhitelist")
  public Set<String> getSpecificDataSourcesToKillUnusedSegmentsIn()
  {
    return specificDataSourcesToKillUnusedSegmentsIn;
  }

  @JsonProperty("killAllDataSources")
  public boolean isKillUnusedSegmentsInAllDataSources()
  {
    return killUnusedSegmentsInAllDataSources;
  }

  @JsonProperty("killPendingSegmentsSkipList")
  public Set<String> getDataSourcesToNotKillStalePendingSegmentsIn()
  {
    return dataSourcesToNotKillStalePendingSegmentsIn;
  }

  @JsonProperty
  public int getMaxSegmentsInNodeLoadingQueue()
  {
    return maxSegmentsInNodeLoadingQueue;
  }

  /**
   * List of historical servers to 'decommission'. Coordinator will not assign new segments to 'decommissioning'
   * servers, and segments will be moved away from them to be placed on non-decommissioning servers at the maximum rate
   * specified by {@link CoordinatorDynamicConfig#getDecommissioningMaxPercentOfMaxSegmentsToMove}.
   *
   * @return list of host:port entries
   */
  @JsonProperty
  public Set<String> getDecommissioningNodes()
  {
    return decommissioningNodes;

  }

  /**
   * The percent of {@link CoordinatorDynamicConfig#getMaxSegmentsToMove()} that determines the maximum number of
   * segments that may be moved away from 'decommissioning' servers (specified by
   * {@link CoordinatorDynamicConfig#getDecommissioningNodes()}) to non-decommissioning servers during one Coordinator
   * balancer run. If this value is 0, segments will neither be moved from or to 'decommissioning' servers, effectively
   * putting them in a sort of "maintenance" mode that will not participate in balancing or assignment by load rules.
   * Decommissioning can also become stalled if there are no available active servers to place the segments. By
   * adjusting this value, an operator can prevent active servers from overload by prioritizing balancing, or
   * decrease decommissioning time instead.
   *
   * @return number in range [0, 100]
   */
  @Min(0)
  @Max(100)
  @JsonProperty
  public int getDecommissioningMaxPercentOfMaxSegmentsToMove()
  {
    return decommissioningMaxPercentOfMaxSegmentsToMove;
  }

  @Override
  public String toString()
  {
    return "CoordinatorDynamicConfig{" +
           "leadingTimeMillisBeforeCanMarkAsUnusedOvershadowedSegments="
           + leadingTimeMillisBeforeCanMarkAsUnusedOvershadowedSegments +
           ", mergeBytesLimit=" + mergeBytesLimit +
           ", mergeSegmentsLimit=" + mergeSegmentsLimit +
           ", maxSegmentsToMove=" + maxSegmentsToMove +
           ", replicantLifetime=" + replicantLifetime +
           ", replicationThrottleLimit=" + replicationThrottleLimit +
           ", balancerComputeThreads=" + balancerComputeThreads +
           ", emitBalancingStats=" + emitBalancingStats +
           ", killUnusedSegmentsInAllDataSources=" + killUnusedSegmentsInAllDataSources +
           ", specificDataSourcesToKillUnusedSegmentsIn=" + specificDataSourcesToKillUnusedSegmentsIn +
           ", dataSourcesToNotKillStalePendingSegmentsIn=" + dataSourcesToNotKillStalePendingSegmentsIn +
           ", maxSegmentsInNodeLoadingQueue=" + maxSegmentsInNodeLoadingQueue +
           ", decommissioningNodes=" + decommissioningNodes +
           ", decommissioningMaxPercentOfMaxSegmentsToMove=" + decommissioningMaxPercentOfMaxSegmentsToMove +
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

    if (leadingTimeMillisBeforeCanMarkAsUnusedOvershadowedSegments !=
        that.leadingTimeMillisBeforeCanMarkAsUnusedOvershadowedSegments) {
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
    if (killUnusedSegmentsInAllDataSources != that.killUnusedSegmentsInAllDataSources) {
      return false;
    }
    if (maxSegmentsInNodeLoadingQueue != that.maxSegmentsInNodeLoadingQueue) {
      return false;
    }
    if (!Objects.equals(specificDataSourcesToKillUnusedSegmentsIn, that.specificDataSourcesToKillUnusedSegmentsIn)) {
      return false;
    }
    if (!Objects.equals(dataSourcesToNotKillStalePendingSegmentsIn, that.dataSourcesToNotKillStalePendingSegmentsIn)) {
      return false;
    }
    if (!Objects.equals(decommissioningNodes, that.decommissioningNodes)) {
      return false;
    }
    return decommissioningMaxPercentOfMaxSegmentsToMove == that.decommissioningMaxPercentOfMaxSegmentsToMove;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        leadingTimeMillisBeforeCanMarkAsUnusedOvershadowedSegments,
        mergeBytesLimit,
        mergeSegmentsLimit,
        maxSegmentsToMove,
        replicantLifetime,
        replicationThrottleLimit,
        balancerComputeThreads,
        emitBalancingStats,
        killUnusedSegmentsInAllDataSources,
        maxSegmentsInNodeLoadingQueue,
        specificDataSourcesToKillUnusedSegmentsIn,
        dataSourcesToNotKillStalePendingSegmentsIn,
        decommissioningNodes,
        decommissioningMaxPercentOfMaxSegmentsToMove
    );
  }

  public static Builder builder()
  {
    return new Builder();
  }

  public static class Builder
  {
    private static final long DEFAULT_LEADING_TIME_MILLIS_BEFORE_CAN_MARK_AS_UNUSED_OVERSHADOWED_SEGMENTS =
        TimeUnit.MINUTES.toMillis(15);
    private static final long DEFAULT_MERGE_BYTES_LIMIT = 524_288_000L;
    private static final int DEFAULT_MERGE_SEGMENTS_LIMIT = 100;
    private static final int DEFAULT_MAX_SEGMENTS_TO_MOVE = 5;
    private static final int DEFAULT_REPLICANT_LIFETIME = 15;
    private static final int DEFAULT_REPLICATION_THROTTLE_LIMIT = 10;
    private static final int DEFAULT_BALANCER_COMPUTE_THREADS = 1;
    private static final boolean DEFAULT_EMIT_BALANCING_STATS = false;
    private static final boolean DEFAULT_KILL_UNUSED_SEGMENTS_IN_ALL_DATA_SOURCES = false;
    private static final int DEFAULT_MAX_SEGMENTS_IN_NODE_LOADING_QUEUE = 0;
    private static final int DEFAULT_DECOMMISSIONING_MAX_SEGMENTS_TO_MOVE_PERCENT = 70;

    private Long leadingTimeMillisBeforeCanMarkAsUnusedOvershadowedSegments;
    private Long mergeBytesLimit;
    private Integer mergeSegmentsLimit;
    private Integer maxSegmentsToMove;
    private Integer replicantLifetime;
    private Integer replicationThrottleLimit;
    private Boolean emitBalancingStats;
    private Integer balancerComputeThreads;
    private Object specificDataSourcesToKillUnusedSegmentsIn;
    private Boolean killUnusedSegmentsInAllDataSources;
    private Object dataSourcesToNotKillStalePendingSegmentsIn;
    private Integer maxSegmentsInNodeLoadingQueue;
    private Object decommissioningNodes;
    private Integer decommissioningMaxPercentOfMaxSegmentsToMove;

    public Builder()
    {
    }

    @JsonCreator
    public Builder(
        @JsonProperty("millisToWaitBeforeDeleting")
        @Nullable Long leadingTimeMillisBeforeCanMarkAsUnusedOvershadowedSegments,
        @JsonProperty("mergeBytesLimit") @Nullable Long mergeBytesLimit,
        @JsonProperty("mergeSegmentsLimit") @Nullable Integer mergeSegmentsLimit,
        @JsonProperty("maxSegmentsToMove") @Nullable Integer maxSegmentsToMove,
        @JsonProperty("replicantLifetime") @Nullable Integer replicantLifetime,
        @JsonProperty("replicationThrottleLimit") @Nullable Integer replicationThrottleLimit,
        @JsonProperty("balancerComputeThreads") @Nullable Integer balancerComputeThreads,
        @JsonProperty("emitBalancingStats") @Nullable Boolean emitBalancingStats,
        @JsonProperty("killDataSourceWhitelist") @Nullable Object specificDataSourcesToKillUnusedSegmentsIn,
        @JsonProperty("killAllDataSources") @Nullable Boolean killUnusedSegmentsInAllDataSources,
        @JsonProperty("killPendingSegmentsSkipList") @Nullable Object dataSourcesToNotKillStalePendingSegmentsIn,
        @JsonProperty("maxSegmentsInNodeLoadingQueue") @Nullable Integer maxSegmentsInNodeLoadingQueue,
        @JsonProperty("decommissioningNodes") @Nullable Object decommissioningNodes,
        @JsonProperty("decommissioningMaxPercentOfMaxSegmentsToMove")
        @Nullable Integer decommissioningMaxPercentOfMaxSegmentsToMove
    )
    {
      this.leadingTimeMillisBeforeCanMarkAsUnusedOvershadowedSegments =
          leadingTimeMillisBeforeCanMarkAsUnusedOvershadowedSegments;
      this.mergeBytesLimit = mergeBytesLimit;
      this.mergeSegmentsLimit = mergeSegmentsLimit;
      this.maxSegmentsToMove = maxSegmentsToMove;
      this.replicantLifetime = replicantLifetime;
      this.replicationThrottleLimit = replicationThrottleLimit;
      this.balancerComputeThreads = balancerComputeThreads;
      this.emitBalancingStats = emitBalancingStats;
      this.specificDataSourcesToKillUnusedSegmentsIn = specificDataSourcesToKillUnusedSegmentsIn;
      this.killUnusedSegmentsInAllDataSources = killUnusedSegmentsInAllDataSources;
      this.dataSourcesToNotKillStalePendingSegmentsIn = dataSourcesToNotKillStalePendingSegmentsIn;
      this.maxSegmentsInNodeLoadingQueue = maxSegmentsInNodeLoadingQueue;
      this.decommissioningNodes = decommissioningNodes;
      this.decommissioningMaxPercentOfMaxSegmentsToMove = decommissioningMaxPercentOfMaxSegmentsToMove;
    }

    public Builder withLeadingTimeMillisBeforeCanMarkAsUnusedOvershadowedSegments(long leadingTimeMillis)
    {
      this.leadingTimeMillisBeforeCanMarkAsUnusedOvershadowedSegments = leadingTimeMillis;
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

    public Builder withSpecificDataSourcesToKillUnusedSegmentsIn(Set<String> dataSources)
    {
      this.specificDataSourcesToKillUnusedSegmentsIn = dataSources;
      return this;
    }

    public Builder withKillUnusedSegmentsInAllDataSources(boolean killUnusedSegmentsInAllDataSources)
    {
      this.killUnusedSegmentsInAllDataSources = killUnusedSegmentsInAllDataSources;
      return this;
    }

    public Builder withMaxSegmentsInNodeLoadingQueue(int maxSegmentsInNodeLoadingQueue)
    {
      this.maxSegmentsInNodeLoadingQueue = maxSegmentsInNodeLoadingQueue;
      return this;
    }

    public Builder withDecommissioningNodes(Set<String> decommissioning)
    {
      this.decommissioningNodes = decommissioning;
      return this;
    }

    public Builder withDecommissioningMaxPercentOfMaxSegmentsToMove(Integer percent)
    {
      this.decommissioningMaxPercentOfMaxSegmentsToMove = percent;
      return this;
    }

    public CoordinatorDynamicConfig build()
    {
      return new CoordinatorDynamicConfig(
          leadingTimeMillisBeforeCanMarkAsUnusedOvershadowedSegments == null
          ? DEFAULT_LEADING_TIME_MILLIS_BEFORE_CAN_MARK_AS_UNUSED_OVERSHADOWED_SEGMENTS
          : leadingTimeMillisBeforeCanMarkAsUnusedOvershadowedSegments,
          mergeBytesLimit == null ? DEFAULT_MERGE_BYTES_LIMIT : mergeBytesLimit,
          mergeSegmentsLimit == null ? DEFAULT_MERGE_SEGMENTS_LIMIT : mergeSegmentsLimit,
          maxSegmentsToMove == null ? DEFAULT_MAX_SEGMENTS_TO_MOVE : maxSegmentsToMove,
          replicantLifetime == null ? DEFAULT_REPLICANT_LIFETIME : replicantLifetime,
          replicationThrottleLimit == null ? DEFAULT_REPLICATION_THROTTLE_LIMIT : replicationThrottleLimit,
          balancerComputeThreads == null ? DEFAULT_BALANCER_COMPUTE_THREADS : balancerComputeThreads,
          emitBalancingStats == null ? DEFAULT_EMIT_BALANCING_STATS : emitBalancingStats,
          specificDataSourcesToKillUnusedSegmentsIn,
          killUnusedSegmentsInAllDataSources == null
          ? DEFAULT_KILL_UNUSED_SEGMENTS_IN_ALL_DATA_SOURCES
          : killUnusedSegmentsInAllDataSources,
          dataSourcesToNotKillStalePendingSegmentsIn,
          maxSegmentsInNodeLoadingQueue == null
          ? DEFAULT_MAX_SEGMENTS_IN_NODE_LOADING_QUEUE
          : maxSegmentsInNodeLoadingQueue,
          decommissioningNodes,
          decommissioningMaxPercentOfMaxSegmentsToMove == null
          ? DEFAULT_DECOMMISSIONING_MAX_SEGMENTS_TO_MOVE_PERCENT
          : decommissioningMaxPercentOfMaxSegmentsToMove
      );
    }

    public CoordinatorDynamicConfig build(CoordinatorDynamicConfig defaults)
    {
      return new CoordinatorDynamicConfig(
          leadingTimeMillisBeforeCanMarkAsUnusedOvershadowedSegments == null
          ? defaults.getLeadingTimeMillisBeforeCanMarkAsUnusedOvershadowedSegments()
          : leadingTimeMillisBeforeCanMarkAsUnusedOvershadowedSegments,
          mergeBytesLimit == null ? defaults.getMergeBytesLimit() : mergeBytesLimit,
          mergeSegmentsLimit == null ? defaults.getMergeSegmentsLimit() : mergeSegmentsLimit,
          maxSegmentsToMove == null ? defaults.getMaxSegmentsToMove() : maxSegmentsToMove,
          replicantLifetime == null ? defaults.getReplicantLifetime() : replicantLifetime,
          replicationThrottleLimit == null ? defaults.getReplicationThrottleLimit() : replicationThrottleLimit,
          balancerComputeThreads == null ? defaults.getBalancerComputeThreads() : balancerComputeThreads,
          emitBalancingStats == null ? defaults.emitBalancingStats() : emitBalancingStats,
          specificDataSourcesToKillUnusedSegmentsIn == null
          ? defaults.getSpecificDataSourcesToKillUnusedSegmentsIn()
          : specificDataSourcesToKillUnusedSegmentsIn,
          killUnusedSegmentsInAllDataSources == null
          ? defaults.isKillUnusedSegmentsInAllDataSources()
          : killUnusedSegmentsInAllDataSources,
          dataSourcesToNotKillStalePendingSegmentsIn == null
          ? defaults.getDataSourcesToNotKillStalePendingSegmentsIn()
          : dataSourcesToNotKillStalePendingSegmentsIn,
          maxSegmentsInNodeLoadingQueue == null
          ? defaults.getMaxSegmentsInNodeLoadingQueue()
          : maxSegmentsInNodeLoadingQueue,
          decommissioningNodes == null ? defaults.getDecommissioningNodes() : decommissioningNodes,
          decommissioningMaxPercentOfMaxSegmentsToMove == null
          ? defaults.getDecommissioningMaxPercentOfMaxSegmentsToMove()
          : decommissioningMaxPercentOfMaxSegmentsToMove
      );
    }
  }
}
