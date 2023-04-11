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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.common.config.JacksonConfigManager;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.duty.KillUnusedSegments;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
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
  @Deprecated
  private final double percentOfSegmentsToConsiderPerMove;
  @Deprecated
  private final boolean useBatchedSegmentSampler;
  private final int replicantLifetime;
  private final int replicationThrottleLimit;
  private final int balancerComputeThreads;
  private final boolean emitBalancingStats;
  private final boolean useRoundRobinSegmentAssignment;

  /**
   * List of specific data sources for which kill tasks are sent in {@link KillUnusedSegments}.
   */
  private final Set<String> specificDataSourcesToKillUnusedSegmentsIn;
  private final Set<String> decommissioningNodes;

  @Deprecated
  private final int decommissioningMaxPercentOfMaxSegmentsToMove;

  /**
   * Stale pending segments belonging to the data sources in this list are not killed by {@link
   * KillStalePendingSegments}. In other words, segments in these data sources are "protected".
   * <p>
   * Pending segments are considered "stale" when their created_time is older than {@link
   * KillStalePendingSegments#KEEP_PENDING_SEGMENTS_OFFSET} from now.
   */
  private final Set<String> dataSourcesToNotKillStalePendingSegmentsIn;

  /**
   * The maximum number of segments that could be queued for loading to any given server.
   * Default values is 0 with the meaning of "unbounded" (any number of
   * segments could be added to the loading queue for any server).
   * See {@link LoadQueuePeon}, {@link org.apache.druid.server.coordinator.rules.LoadRule#run}
   */
  private final int maxSegmentsInNodeLoadingQueue;
  private final boolean pauseCoordination;

  /**
   * This decides whether additional replication is needed for segments that have failed to load due to a load timeout.
   * When enabled, the coordinator will attempt to replicate the failed segment on a different historical server.
   * The historical which failed to load the segment may still load the segment later. Therefore, enabling this setting
   * works better if there are a few slow historicals in the cluster and segment availability needs to be sped up.
   */
  private final boolean replicateAfterLoadTimeout;

  /**
   * This is the maximum number of non-primary segment replicants to load per Coordination run. This number can
   * be set to put a hard upper limit on the number of replicants loaded. It is a tool that can help prevent
   * long delays in new data loads after events such as a Historical server leaving the cluster.
   */
  @Deprecated
  private final int maxNonPrimaryReplicantsToLoad;

  private static final Logger log = new Logger(CoordinatorDynamicConfig.class);

  @JsonCreator
  public CoordinatorDynamicConfig(
      // Keeping the legacy 'millisToWaitBeforeDeleting' property name for backward compatibility. When the project is
      // updated to Jackson 2.9 it could be changed, see https://github.com/apache/druid/issues/7152
      @JsonProperty("millisToWaitBeforeDeleting")
          long leadingTimeMillisBeforeCanMarkAsUnusedOvershadowedSegments,
      @JsonProperty("mergeBytesLimit") long mergeBytesLimit,
      @JsonProperty("mergeSegmentsLimit") int mergeSegmentsLimit,
      @JsonProperty("maxSegmentsToMove") int maxSegmentsToMove,
      @Deprecated @JsonProperty("percentOfSegmentsToConsiderPerMove") @Nullable Double percentOfSegmentsToConsiderPerMove,
      @Deprecated @JsonProperty("useBatchedSegmentSampler") Boolean useBatchedSegmentSampler,
      @JsonProperty("replicantLifetime") int replicantLifetime,
      @JsonProperty("replicationThrottleLimit") int replicationThrottleLimit,
      @JsonProperty("balancerComputeThreads") int balancerComputeThreads,
      @JsonProperty("emitBalancingStats") boolean emitBalancingStats,
      // Type is Object here so that we can support both string and list as Coordinator console can not send array of
      // strings in the update request. See https://github.com/apache/druid/issues/3055.
      // Keeping the legacy 'killDataSourceWhitelist' property name for backward compatibility. When the project is
      // updated to Jackson 2.9 it could be changed, see https://github.com/apache/druid/issues/7152
      @JsonProperty("killDataSourceWhitelist") Object specificDataSourcesToKillUnusedSegmentsIn,
      // Type is Object here so that we can support both string and list as Coordinator console can not send array of
      // strings in the update request, as well as for specificDataSourcesToKillUnusedSegmentsIn.
      // Keeping the legacy 'killPendingSegmentsSkipList' property name for backward compatibility. When the project is
      // updated to Jackson 2.9 it could be changed, see https://github.com/apache/druid/issues/7152
      @JsonProperty("killPendingSegmentsSkipList") Object dataSourcesToNotKillStalePendingSegmentsIn,
      @JsonProperty("maxSegmentsInNodeLoadingQueue") @Nullable Integer maxSegmentsInNodeLoadingQueue,
      @JsonProperty("decommissioningNodes") Object decommissioningNodes,
      @JsonProperty("decommissioningMaxPercentOfMaxSegmentsToMove") int decommissioningMaxPercentOfMaxSegmentsToMove,
      @JsonProperty("pauseCoordination") boolean pauseCoordination,
      @JsonProperty("replicateAfterLoadTimeout") boolean replicateAfterLoadTimeout,
      @JsonProperty("maxNonPrimaryReplicantsToLoad") @Nullable Integer maxNonPrimaryReplicantsToLoad,
      @JsonProperty("useRoundRobinSegmentAssignment") @Nullable Boolean useRoundRobinSegmentAssignment
  )
  {
    this.leadingTimeMillisBeforeCanMarkAsUnusedOvershadowedSegments =
        leadingTimeMillisBeforeCanMarkAsUnusedOvershadowedSegments;
    this.mergeBytesLimit = mergeBytesLimit;
    this.mergeSegmentsLimit = mergeSegmentsLimit;
    this.maxSegmentsToMove = maxSegmentsToMove;

    if (percentOfSegmentsToConsiderPerMove == null) {
      log.debug(
          "percentOfSegmentsToConsiderPerMove was null! This is likely because your metastore does not "
          + "reflect this configuration being added to Druid in a recent release. Druid is defaulting the value "
          + "to the Druid default of %f. It is recommended that you re-submit your dynamic config with your "
          + "desired value for percentOfSegmentsToConsideredPerMove",
          Defaults.PERCENT_OF_SEGMENTS_TO_CONSIDER_PER_MOVE
      );
      percentOfSegmentsToConsiderPerMove = Defaults.PERCENT_OF_SEGMENTS_TO_CONSIDER_PER_MOVE;
    }
    Preconditions.checkArgument(
        percentOfSegmentsToConsiderPerMove > 0 && percentOfSegmentsToConsiderPerMove <= 100,
        "percentOfSegmentsToConsiderPerMove should be between 1 and 100!"
    );
    this.percentOfSegmentsToConsiderPerMove = percentOfSegmentsToConsiderPerMove;

    this.useBatchedSegmentSampler = Builder.valueOrDefault(
        useBatchedSegmentSampler,
        Defaults.USE_BATCHED_SEGMENT_SAMPLER
    );

    this.replicantLifetime = replicantLifetime;
    this.replicationThrottleLimit = replicationThrottleLimit;
    this.balancerComputeThreads = Math.max(balancerComputeThreads, 1);
    this.emitBalancingStats = emitBalancingStats;
    this.specificDataSourcesToKillUnusedSegmentsIn = parseJsonStringOrArray(specificDataSourcesToKillUnusedSegmentsIn);
    this.dataSourcesToNotKillStalePendingSegmentsIn =
        parseJsonStringOrArray(dataSourcesToNotKillStalePendingSegmentsIn);
    this.maxSegmentsInNodeLoadingQueue = Builder.valueOrDefault(
        maxSegmentsInNodeLoadingQueue,
        Defaults.MAX_SEGMENTS_IN_NODE_LOADING_QUEUE
    );
    this.decommissioningNodes = parseJsonStringOrArray(decommissioningNodes);
    Preconditions.checkArgument(
        decommissioningMaxPercentOfMaxSegmentsToMove >= 0 && decommissioningMaxPercentOfMaxSegmentsToMove <= 100,
        "decommissioningMaxPercentOfMaxSegmentsToMove should be in range [0, 100]"
    );
    this.decommissioningMaxPercentOfMaxSegmentsToMove = decommissioningMaxPercentOfMaxSegmentsToMove;

    this.pauseCoordination = pauseCoordination;
    this.replicateAfterLoadTimeout = replicateAfterLoadTimeout;

    if (maxNonPrimaryReplicantsToLoad == null) {
      log.debug(
          "maxNonPrimaryReplicantsToLoad was null! This is likely because your metastore does not "
          + "reflect this configuration being added to Druid in a recent release. Druid is defaulting the value "
          + "to the Druid default of %d. It is recommended that you re-submit your dynamic config with your "
          + "desired value for maxNonPrimaryReplicantsToLoad",
          Defaults.MAX_NON_PRIMARY_REPLICANTS_TO_LOAD
      );
      maxNonPrimaryReplicantsToLoad = Defaults.MAX_NON_PRIMARY_REPLICANTS_TO_LOAD;
    }
    Preconditions.checkArgument(
        maxNonPrimaryReplicantsToLoad >= 0,
        "maxNonPrimaryReplicantsToLoad must be greater than or equal to 0."
    );
    this.maxNonPrimaryReplicantsToLoad = maxNonPrimaryReplicantsToLoad;

    this.useRoundRobinSegmentAssignment = Builder.valueOrDefault(
        useRoundRobinSegmentAssignment,
        Defaults.USE_ROUND_ROBIN_ASSIGNMENT
    );
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

  @Deprecated
  @JsonProperty
  public double getPercentOfSegmentsToConsiderPerMove()
  {
    return percentOfSegmentsToConsiderPerMove;
  }

  @Deprecated
  @JsonProperty
  public boolean useBatchedSegmentSampler()
  {
    return useBatchedSegmentSampler;
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

  @JsonIgnore
  public boolean isKillUnusedSegmentsInAllDataSources()
  {
    return specificDataSourcesToKillUnusedSegmentsIn.isEmpty();
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

  @JsonProperty
  public boolean isUseRoundRobinSegmentAssignment()
  {
    return useRoundRobinSegmentAssignment;
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
  @Deprecated
  @JsonProperty
  public int getDecommissioningMaxPercentOfMaxSegmentsToMove()
  {
    return decommissioningMaxPercentOfMaxSegmentsToMove;
  }

  @JsonProperty
  public boolean getPauseCoordination()
  {
    return pauseCoordination;
  }

  @JsonProperty
  public boolean getReplicateAfterLoadTimeout()
  {
    return replicateAfterLoadTimeout;
  }

  @Min(0)
  @Deprecated
  @JsonProperty
  public int getMaxNonPrimaryReplicantsToLoad()
  {
    return maxNonPrimaryReplicantsToLoad;
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
           ", percentOfSegmentsToConsiderPerMove=" + percentOfSegmentsToConsiderPerMove +
           ", useBatchedSegmentSampler=" + useBatchedSegmentSampler +
           ", replicantLifetime=" + replicantLifetime +
           ", replicationThrottleLimit=" + replicationThrottleLimit +
           ", balancerComputeThreads=" + balancerComputeThreads +
           ", emitBalancingStats=" + emitBalancingStats +
           ", specificDataSourcesToKillUnusedSegmentsIn=" + specificDataSourcesToKillUnusedSegmentsIn +
           ", dataSourcesToNotKillStalePendingSegmentsIn=" + dataSourcesToNotKillStalePendingSegmentsIn +
           ", maxSegmentsInNodeLoadingQueue=" + maxSegmentsInNodeLoadingQueue +
           ", decommissioningNodes=" + decommissioningNodes +
           ", decommissioningMaxPercentOfMaxSegmentsToMove=" + decommissioningMaxPercentOfMaxSegmentsToMove +
           ", pauseCoordination=" + pauseCoordination +
           ", replicateAfterLoadTimeout=" + replicateAfterLoadTimeout +
           ", maxNonPrimaryReplicantsToLoad=" + maxNonPrimaryReplicantsToLoad +
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

    return leadingTimeMillisBeforeCanMarkAsUnusedOvershadowedSegments
           == that.leadingTimeMillisBeforeCanMarkAsUnusedOvershadowedSegments
           && mergeBytesLimit == that.mergeBytesLimit
           && mergeSegmentsLimit == that.mergeSegmentsLimit
           && maxSegmentsToMove == that.maxSegmentsToMove
           && percentOfSegmentsToConsiderPerMove == that.percentOfSegmentsToConsiderPerMove
           && decommissioningMaxPercentOfMaxSegmentsToMove == that.decommissioningMaxPercentOfMaxSegmentsToMove
           && useBatchedSegmentSampler == that.useBatchedSegmentSampler
           && balancerComputeThreads == that.balancerComputeThreads
           && emitBalancingStats == that.emitBalancingStats
           && replicantLifetime == that.replicantLifetime
           && replicationThrottleLimit == that.replicationThrottleLimit
           && replicateAfterLoadTimeout == that.replicateAfterLoadTimeout
           && maxSegmentsInNodeLoadingQueue == that.maxSegmentsInNodeLoadingQueue
           && maxNonPrimaryReplicantsToLoad == that.maxNonPrimaryReplicantsToLoad
           && useRoundRobinSegmentAssignment == that.useRoundRobinSegmentAssignment
           && pauseCoordination == that.pauseCoordination
           && Objects.equals(
               specificDataSourcesToKillUnusedSegmentsIn,
               that.specificDataSourcesToKillUnusedSegmentsIn)
           && Objects.equals(
               dataSourcesToNotKillStalePendingSegmentsIn,
               that.dataSourcesToNotKillStalePendingSegmentsIn)
           && Objects.equals(decommissioningNodes, that.decommissioningNodes);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        leadingTimeMillisBeforeCanMarkAsUnusedOvershadowedSegments,
        mergeBytesLimit,
        mergeSegmentsLimit,
        maxSegmentsToMove,
        percentOfSegmentsToConsiderPerMove,
        useBatchedSegmentSampler,
        replicantLifetime,
        replicationThrottleLimit,
        balancerComputeThreads,
        emitBalancingStats,
        maxSegmentsInNodeLoadingQueue,
        specificDataSourcesToKillUnusedSegmentsIn,
        dataSourcesToNotKillStalePendingSegmentsIn,
        decommissioningNodes,
        decommissioningMaxPercentOfMaxSegmentsToMove,
        pauseCoordination,
        maxNonPrimaryReplicantsToLoad
    );
  }

  public static Builder builder()
  {
    return new Builder();
  }

  private static class Defaults
  {
    static final long LEADING_MILLIS_BEFORE_MARK_UNUSED = TimeUnit.MINUTES.toMillis(15);
    static final long MERGE_BYTES_LIMIT = 524_288_000L;
    static final int MERGE_SEGMENTS_LIMIT = 100;
    static final int MAX_SEGMENTS_TO_MOVE = 500;
    static final double PERCENT_OF_SEGMENTS_TO_CONSIDER_PER_MOVE = 100;
    static final int REPLICANT_LIFETIME = 15;
    static final int REPLICATION_THROTTLE_LIMIT = 10;
    static final int BALANCER_COMPUTE_THREADS = 1;
    static final boolean EMIT_BALANCING_STATS = false;
    static final boolean USE_BATCHED_SEGMENT_SAMPLER = true;
    static final int MAX_SEGMENTS_IN_NODE_LOADING_QUEUE = 100;
    static final int DECOMMISSIONING_MAX_SEGMENTS_TO_MOVE_PERCENT = 70;
    static final boolean PAUSE_COORDINATION = false;
    static final boolean REPLICATE_AFTER_LOAD_TIMEOUT = false;
    static final int MAX_NON_PRIMARY_REPLICANTS_TO_LOAD = Integer.MAX_VALUE;
    static final boolean USE_ROUND_ROBIN_ASSIGNMENT = true;
  }

  public static class Builder
  {
    private Long leadingTimeMillisBeforeCanMarkAsUnusedOvershadowedSegments;
    private Long mergeBytesLimit;
    private Integer mergeSegmentsLimit;
    private Integer maxSegmentsToMove;
    private Double percentOfSegmentsToConsiderPerMove;
    private Boolean useBatchedSegmentSampler;
    private Integer replicantLifetime;
    private Integer replicationThrottleLimit;
    private Boolean emitBalancingStats;
    private Integer balancerComputeThreads;
    private Object specificDataSourcesToKillUnusedSegmentsIn;
    private Object dataSourcesToNotKillStalePendingSegmentsIn;
    private Integer maxSegmentsInNodeLoadingQueue;
    private Object decommissioningNodes;
    private Integer decommissioningMaxPercentOfMaxSegmentsToMove;
    private Boolean pauseCoordination;
    private Boolean replicateAfterLoadTimeout;
    private Integer maxNonPrimaryReplicantsToLoad;
    private Boolean useRoundRobinSegmentAssignment;

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
        @Deprecated @JsonProperty("percentOfSegmentsToConsiderPerMove") @Nullable Double percentOfSegmentsToConsiderPerMove,
        @Deprecated @JsonProperty("useBatchedSegmentSampler") Boolean useBatchedSegmentSampler,
        @JsonProperty("replicantLifetime") @Nullable Integer replicantLifetime,
        @JsonProperty("replicationThrottleLimit") @Nullable Integer replicationThrottleLimit,
        @JsonProperty("balancerComputeThreads") @Nullable Integer balancerComputeThreads,
        @JsonProperty("emitBalancingStats") @Nullable Boolean emitBalancingStats,
        @JsonProperty("killDataSourceWhitelist") @Nullable Object specificDataSourcesToKillUnusedSegmentsIn,
        @JsonProperty("killPendingSegmentsSkipList") @Nullable Object dataSourcesToNotKillStalePendingSegmentsIn,
        @JsonProperty("maxSegmentsInNodeLoadingQueue") @Nullable Integer maxSegmentsInNodeLoadingQueue,
        @JsonProperty("decommissioningNodes") @Nullable Object decommissioningNodes,
        @JsonProperty("decommissioningMaxPercentOfMaxSegmentsToMove")
        @Nullable Integer decommissioningMaxPercentOfMaxSegmentsToMove,
        @JsonProperty("pauseCoordination") @Nullable Boolean pauseCoordination,
        @JsonProperty("replicateAfterLoadTimeout") @Nullable Boolean replicateAfterLoadTimeout,
        @JsonProperty("maxNonPrimaryReplicantsToLoad") @Nullable Integer maxNonPrimaryReplicantsToLoad,
        @JsonProperty("useRoundRobinSegmentAssignment") @Nullable Boolean useRoundRobinSegmentAssignment
    )
    {
      this.leadingTimeMillisBeforeCanMarkAsUnusedOvershadowedSegments =
          leadingTimeMillisBeforeCanMarkAsUnusedOvershadowedSegments;
      this.mergeBytesLimit = mergeBytesLimit;
      this.mergeSegmentsLimit = mergeSegmentsLimit;
      this.maxSegmentsToMove = maxSegmentsToMove;
      this.percentOfSegmentsToConsiderPerMove = percentOfSegmentsToConsiderPerMove;
      this.useBatchedSegmentSampler = useBatchedSegmentSampler;
      this.replicantLifetime = replicantLifetime;
      this.replicationThrottleLimit = replicationThrottleLimit;
      this.balancerComputeThreads = balancerComputeThreads;
      this.emitBalancingStats = emitBalancingStats;
      this.specificDataSourcesToKillUnusedSegmentsIn = specificDataSourcesToKillUnusedSegmentsIn;
      this.dataSourcesToNotKillStalePendingSegmentsIn = dataSourcesToNotKillStalePendingSegmentsIn;
      this.maxSegmentsInNodeLoadingQueue = maxSegmentsInNodeLoadingQueue;
      this.decommissioningNodes = decommissioningNodes;
      this.decommissioningMaxPercentOfMaxSegmentsToMove = decommissioningMaxPercentOfMaxSegmentsToMove;
      this.pauseCoordination = pauseCoordination;
      this.replicateAfterLoadTimeout = replicateAfterLoadTimeout;
      this.maxNonPrimaryReplicantsToLoad = maxNonPrimaryReplicantsToLoad;
      this.useRoundRobinSegmentAssignment = useRoundRobinSegmentAssignment;
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

    @Deprecated
    public Builder withPercentOfSegmentsToConsiderPerMove(double percentOfSegmentsToConsiderPerMove)
    {
      this.percentOfSegmentsToConsiderPerMove = percentOfSegmentsToConsiderPerMove;
      return this;
    }

    @Deprecated
    public Builder withUseBatchedSegmentSampler(boolean useBatchedSegmentSampler)
    {
      this.useBatchedSegmentSampler = useBatchedSegmentSampler;
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

    public Builder withPauseCoordination(boolean pauseCoordination)
    {
      this.pauseCoordination = pauseCoordination;
      return this;
    }

    public Builder withReplicateAfterLoadTimeout(boolean replicateAfterLoadTimeout)
    {
      this.replicateAfterLoadTimeout = replicateAfterLoadTimeout;
      return this;
    }

    public Builder withMaxNonPrimaryReplicantsToLoad(int maxNonPrimaryReplicantsToLoad)
    {
      this.maxNonPrimaryReplicantsToLoad = maxNonPrimaryReplicantsToLoad;
      return this;
    }

    public Builder withUseRoundRobinSegmentAssignment(boolean useRoundRobinSegmentAssignment)
    {
      this.useRoundRobinSegmentAssignment = useRoundRobinSegmentAssignment;
      return this;
    }

    /**
     * Builds a CoordinatoryDynamicConfig using either the configured values, or
     * the default value if not configured.
     */
    public CoordinatorDynamicConfig build()
    {
      return new CoordinatorDynamicConfig(
          valueOrDefault(
              leadingTimeMillisBeforeCanMarkAsUnusedOvershadowedSegments,
              Defaults.LEADING_MILLIS_BEFORE_MARK_UNUSED
          ),
          valueOrDefault(mergeBytesLimit, Defaults.MERGE_BYTES_LIMIT),
          valueOrDefault(mergeSegmentsLimit, Defaults.MERGE_SEGMENTS_LIMIT),
          valueOrDefault(maxSegmentsToMove, Defaults.MAX_SEGMENTS_TO_MOVE),
          valueOrDefault(percentOfSegmentsToConsiderPerMove, Defaults.PERCENT_OF_SEGMENTS_TO_CONSIDER_PER_MOVE),
          valueOrDefault(useBatchedSegmentSampler, Defaults.USE_BATCHED_SEGMENT_SAMPLER),
          valueOrDefault(replicantLifetime, Defaults.REPLICANT_LIFETIME),
          valueOrDefault(replicationThrottleLimit, Defaults.REPLICATION_THROTTLE_LIMIT),
          valueOrDefault(balancerComputeThreads, Defaults.BALANCER_COMPUTE_THREADS),
          valueOrDefault(emitBalancingStats, Defaults.EMIT_BALANCING_STATS),
          specificDataSourcesToKillUnusedSegmentsIn,
          dataSourcesToNotKillStalePendingSegmentsIn,
          valueOrDefault(maxSegmentsInNodeLoadingQueue, Defaults.MAX_SEGMENTS_IN_NODE_LOADING_QUEUE),
          decommissioningNodes,
          valueOrDefault(
              decommissioningMaxPercentOfMaxSegmentsToMove,
              Defaults.DECOMMISSIONING_MAX_SEGMENTS_TO_MOVE_PERCENT
          ),
          valueOrDefault(pauseCoordination, Defaults.PAUSE_COORDINATION),
          valueOrDefault(replicateAfterLoadTimeout, Defaults.REPLICATE_AFTER_LOAD_TIMEOUT),
          valueOrDefault(maxNonPrimaryReplicantsToLoad, Defaults.MAX_NON_PRIMARY_REPLICANTS_TO_LOAD),
          valueOrDefault(useRoundRobinSegmentAssignment, Defaults.USE_ROUND_ROBIN_ASSIGNMENT)
      );
    }

    private static <T> T valueOrDefault(@Nullable T value, @NotNull T defaultValue)
    {
      return value == null ? defaultValue : value;
    }

    public CoordinatorDynamicConfig build(CoordinatorDynamicConfig defaults)
    {
      return new CoordinatorDynamicConfig(
          valueOrDefault(
              leadingTimeMillisBeforeCanMarkAsUnusedOvershadowedSegments,
              defaults.getLeadingTimeMillisBeforeCanMarkAsUnusedOvershadowedSegments()
          ),
          valueOrDefault(mergeBytesLimit, defaults.getMergeBytesLimit()),
          valueOrDefault(mergeSegmentsLimit, defaults.getMergeSegmentsLimit()),
          valueOrDefault(maxSegmentsToMove, defaults.getMaxSegmentsToMove()),
          valueOrDefault(percentOfSegmentsToConsiderPerMove, defaults.getPercentOfSegmentsToConsiderPerMove()),
          valueOrDefault(useBatchedSegmentSampler, defaults.useBatchedSegmentSampler()),
          valueOrDefault(replicantLifetime, defaults.getReplicantLifetime()),
          valueOrDefault(replicationThrottleLimit, defaults.getReplicationThrottleLimit()),
          valueOrDefault(balancerComputeThreads, defaults.getBalancerComputeThreads()),
          valueOrDefault(emitBalancingStats, defaults.emitBalancingStats()),
          valueOrDefault(specificDataSourcesToKillUnusedSegmentsIn, defaults.getSpecificDataSourcesToKillUnusedSegmentsIn()),
          valueOrDefault(dataSourcesToNotKillStalePendingSegmentsIn, defaults.getDataSourcesToNotKillStalePendingSegmentsIn()),
          valueOrDefault(maxSegmentsInNodeLoadingQueue, defaults.getMaxSegmentsInNodeLoadingQueue()),
          valueOrDefault(decommissioningNodes, defaults.getDecommissioningNodes()),
          valueOrDefault(
              decommissioningMaxPercentOfMaxSegmentsToMove,
              defaults.getDecommissioningMaxPercentOfMaxSegmentsToMove()
          ),
          valueOrDefault(pauseCoordination, defaults.getPauseCoordination()),
          valueOrDefault(replicateAfterLoadTimeout, defaults.getReplicateAfterLoadTimeout()),
          valueOrDefault(maxNonPrimaryReplicantsToLoad, defaults.getMaxNonPrimaryReplicantsToLoad()),
          valueOrDefault(useRoundRobinSegmentAssignment, defaults.isUseRoundRobinSegmentAssignment())
      );
    }
  }
}
