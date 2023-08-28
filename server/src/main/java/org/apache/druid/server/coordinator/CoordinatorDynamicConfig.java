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
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.duty.KillUnusedSegments;
import org.apache.druid.server.coordinator.loading.LoadQueuePeon;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.utils.JvmUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
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

  private final long markSegmentAsUnusedDelayMillis;
  private final long mergeBytesLimit;
  private final int mergeSegmentsLimit;
  private final int maxSegmentsToMove;
  private final int replicantLifetime;
  private final int replicationThrottleLimit;
  private final int balancerComputeThreads;
  private final boolean useRoundRobinSegmentAssignment;
  private final boolean smartSegmentLoading;

  /**
   * List of specific data sources for which kill tasks are sent in {@link KillUnusedSegments}.
   */
  private final Set<String> specificDataSourcesToKillUnusedSegmentsIn;

  private final double killTaskSlotRatio;
  private final int maxKillTaskSlots;
  private final Set<String> decommissioningNodes;

  private final Map<String, String> debugDimensions;
  private final Map<Dimension, String> validDebugDimensions;

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
          long markSegmentAsUnusedDelayMillis,
      @JsonProperty("mergeBytesLimit") long mergeBytesLimit,
      @JsonProperty("mergeSegmentsLimit") int mergeSegmentsLimit,
      @JsonProperty("maxSegmentsToMove") int maxSegmentsToMove,
      @JsonProperty("replicantLifetime") int replicantLifetime,
      @JsonProperty("replicationThrottleLimit") int replicationThrottleLimit,
      @JsonProperty("balancerComputeThreads") int balancerComputeThreads,
      // Type is Object here so that we can support both string and list as Coordinator console can not send array of
      // strings in the update request. See https://github.com/apache/druid/issues/3055.
      // Keeping the legacy 'killDataSourceWhitelist' property name for backward compatibility. When the project is
      // updated to Jackson 2.9 it could be changed, see https://github.com/apache/druid/issues/7152
      @JsonProperty("killDataSourceWhitelist") Object specificDataSourcesToKillUnusedSegmentsIn,
      @JsonProperty("killTaskSlotRatio") @Nullable Double killTaskSlotRatio,
      @JsonProperty("maxKillTaskSlots") @Nullable Integer maxKillTaskSlots,
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
      @JsonProperty("useRoundRobinSegmentAssignment") @Nullable Boolean useRoundRobinSegmentAssignment,
      @JsonProperty("smartSegmentLoading") @Nullable Boolean smartSegmentLoading,
      @JsonProperty("debugDimensions") @Nullable Map<String, String> debugDimensions
  )
  {
    this.markSegmentAsUnusedDelayMillis =
        markSegmentAsUnusedDelayMillis;
    this.mergeBytesLimit = mergeBytesLimit;
    this.mergeSegmentsLimit = mergeSegmentsLimit;
    this.maxSegmentsToMove = maxSegmentsToMove;
    this.smartSegmentLoading = Builder.valueOrDefault(smartSegmentLoading, Defaults.SMART_SEGMENT_LOADING);

    this.replicantLifetime = replicantLifetime;
    this.replicationThrottleLimit = replicationThrottleLimit;
    this.balancerComputeThreads = Math.max(balancerComputeThreads, 1);
    this.specificDataSourcesToKillUnusedSegmentsIn
        = parseJsonStringOrArray(specificDataSourcesToKillUnusedSegmentsIn);
    if (null != killTaskSlotRatio && (killTaskSlotRatio < 0 || killTaskSlotRatio > 1)) {
      throw InvalidInput.exception(
          "killTaskSlotRatio [%.2f] is invalid. It must be >= 0 and <= 1.",
          killTaskSlotRatio
      );
    }
    this.killTaskSlotRatio = killTaskSlotRatio != null ? killTaskSlotRatio : Defaults.KILL_TASK_SLOT_RATIO;
    if (null != maxKillTaskSlots && maxKillTaskSlots < 0) {
      throw InvalidInput.exception(
          "maxKillTaskSlots [%d] is invalid. It must be >= 0.",
          maxKillTaskSlots
      );
    }
    this.maxKillTaskSlots = maxKillTaskSlots != null ? maxKillTaskSlots : Defaults.MAX_KILL_TASK_SLOTS;
    this.dataSourcesToNotKillStalePendingSegmentsIn
        = parseJsonStringOrArray(dataSourcesToNotKillStalePendingSegmentsIn);
    this.maxSegmentsInNodeLoadingQueue = Builder.valueOrDefault(
        maxSegmentsInNodeLoadingQueue,
        Defaults.MAX_SEGMENTS_IN_NODE_LOADING_QUEUE
    );
    this.decommissioningNodes = parseJsonStringOrArray(decommissioningNodes);
    Preconditions.checkArgument(
        decommissioningMaxPercentOfMaxSegmentsToMove >= 0 && decommissioningMaxPercentOfMaxSegmentsToMove <= 100,
        "'decommissioningMaxPercentOfMaxSegmentsToMove' should be in range [0, 100]"
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
    this.debugDimensions = debugDimensions;
    this.validDebugDimensions = validateDebugDimensions(debugDimensions);
  }

  private Map<Dimension, String> validateDebugDimensions(Map<String, String> debugDimensions)
  {
    final Map<Dimension, String> validDebugDimensions = new EnumMap<>(Dimension.class);
    if (debugDimensions == null || debugDimensions.isEmpty()) {
      return validDebugDimensions;
    }

    for (Dimension dimension : Dimension.values()) {
      final String dimensionValue = debugDimensions.get(dimension.reportedName());
      if (dimensionValue != null) {
        validDebugDimensions.put(dimension, dimensionValue);
      }
    }

    return validDebugDimensions;
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
      return ImmutableSet.copyOf((Collection) jsonStringOrArray);
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
  public long getMarkSegmentAsUnusedDelayMillis()
  {
    return markSegmentAsUnusedDelayMillis;
  }

  @JsonProperty
  public long getMergeBytesLimit()
  {
    return mergeBytesLimit;
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

  @JsonProperty("killTaskSlotRatio")
  public double getKillTaskSlotRatio()
  {
    return killTaskSlotRatio;
  }

  @JsonProperty("maxKillTaskSlots")
  public int getMaxKillTaskSlots()
  {
    return maxKillTaskSlots;
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

  @JsonProperty
  public boolean isSmartSegmentLoading()
  {
    return smartSegmentLoading;
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

  @JsonProperty
  public Map<String, String> getDebugDimensions()
  {
    return debugDimensions;
  }

  @JsonIgnore
  public Map<Dimension, String> getValidatedDebugDimensions()
  {
    return validDebugDimensions;
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
           + markSegmentAsUnusedDelayMillis +
           ", mergeBytesLimit=" + mergeBytesLimit +
           ", mergeSegmentsLimit=" + mergeSegmentsLimit +
           ", maxSegmentsToMove=" + maxSegmentsToMove +
           ", replicantLifetime=" + replicantLifetime +
           ", replicationThrottleLimit=" + replicationThrottleLimit +
           ", balancerComputeThreads=" + balancerComputeThreads +
           ", specificDataSourcesToKillUnusedSegmentsIn=" + specificDataSourcesToKillUnusedSegmentsIn +
           ", killTaskSlotRatio=" + killTaskSlotRatio +
           ", maxKillTaskSlots=" + maxKillTaskSlots +
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

    return markSegmentAsUnusedDelayMillis == that.markSegmentAsUnusedDelayMillis
           && mergeBytesLimit == that.mergeBytesLimit
           && mergeSegmentsLimit == that.mergeSegmentsLimit
           && maxSegmentsToMove == that.maxSegmentsToMove
           && decommissioningMaxPercentOfMaxSegmentsToMove == that.decommissioningMaxPercentOfMaxSegmentsToMove
           && balancerComputeThreads == that.balancerComputeThreads
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
           && Objects.equals(killTaskSlotRatio, that.killTaskSlotRatio)
           && Objects.equals(maxKillTaskSlots, that.maxKillTaskSlots)
           && Objects.equals(
               dataSourcesToNotKillStalePendingSegmentsIn,
               that.dataSourcesToNotKillStalePendingSegmentsIn)
           && Objects.equals(decommissioningNodes, that.decommissioningNodes)
           && Objects.equals(debugDimensions, that.debugDimensions);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        markSegmentAsUnusedDelayMillis,
        mergeBytesLimit,
        mergeSegmentsLimit,
        maxSegmentsToMove,
        replicantLifetime,
        replicationThrottleLimit,
        balancerComputeThreads,
        maxSegmentsInNodeLoadingQueue,
        specificDataSourcesToKillUnusedSegmentsIn,
        killTaskSlotRatio,
        maxKillTaskSlots,
        dataSourcesToNotKillStalePendingSegmentsIn,
        decommissioningNodes,
        decommissioningMaxPercentOfMaxSegmentsToMove,
        pauseCoordination,
        maxNonPrimaryReplicantsToLoad,
        debugDimensions
    );
  }

  public static Builder builder()
  {
    return new Builder();
  }

  /**
   * Returns a value of {@code (num processors / 2)} to ensure that balancing
   * computations do not hog all Coordinator resources.
   */
  public static int getDefaultBalancerComputeThreads()
  {
    return Math.max(1, JvmUtils.getRuntimeInfo().getAvailableProcessors() / 2);
  }

  private static class Defaults
  {
    static final long LEADING_MILLIS_BEFORE_MARK_UNUSED = TimeUnit.MINUTES.toMillis(15);
    static final long MERGE_BYTES_LIMIT = 524_288_000L;
    static final int MERGE_SEGMENTS_LIMIT = 100;
    static final int MAX_SEGMENTS_TO_MOVE = 100;
    static final int REPLICANT_LIFETIME = 15;
    static final int REPLICATION_THROTTLE_LIMIT = 500;
    static final int MAX_SEGMENTS_IN_NODE_LOADING_QUEUE = 500;
    static final int DECOMMISSIONING_MAX_SEGMENTS_TO_MOVE_PERCENT = 70;
    static final boolean PAUSE_COORDINATION = false;
    static final boolean REPLICATE_AFTER_LOAD_TIMEOUT = false;
    static final int MAX_NON_PRIMARY_REPLICANTS_TO_LOAD = Integer.MAX_VALUE;
    static final boolean USE_ROUND_ROBIN_ASSIGNMENT = true;
    static final boolean SMART_SEGMENT_LOADING = true;

    // The following default values for killTaskSlotRatio and maxKillTaskSlots
    // are to preserve the behavior before Druid 0.28 and a future version may
    // want to consider better defaults so that kill tasks can not eat up all
    // the capacity in the cluster would be nice
    static final double KILL_TASK_SLOT_RATIO = 1.0;
    static final int MAX_KILL_TASK_SLOTS = Integer.MAX_VALUE;
  }

  public static class Builder
  {
    private Long markSegmentAsUnusedDelayMillis;
    private Long mergeBytesLimit;
    private Integer mergeSegmentsLimit;
    private Integer maxSegmentsToMove;
    private Integer replicantLifetime;
    private Integer replicationThrottleLimit;
    private Integer balancerComputeThreads;
    private Object specificDataSourcesToKillUnusedSegmentsIn;
    private Double killTaskSlotRatio;
    private Integer maxKillTaskSlots;
    private Object dataSourcesToNotKillStalePendingSegmentsIn;
    private Integer maxSegmentsInNodeLoadingQueue;
    private Object decommissioningNodes;
    private Map<String, String> debugDimensions;
    private Integer decommissioningMaxPercentOfMaxSegmentsToMove;
    private Boolean pauseCoordination;
    private Boolean replicateAfterLoadTimeout;
    private Integer maxNonPrimaryReplicantsToLoad;
    private Boolean useRoundRobinSegmentAssignment;
    private Boolean smartSegmentLoading;

    public Builder()
    {
    }

    @JsonCreator
    public Builder(
        @JsonProperty("millisToWaitBeforeDeleting") @Nullable Long markSegmentAsUnusedDelayMillis,
        @JsonProperty("mergeBytesLimit") @Nullable Long mergeBytesLimit,
        @JsonProperty("mergeSegmentsLimit") @Nullable Integer mergeSegmentsLimit,
        @JsonProperty("maxSegmentsToMove") @Nullable Integer maxSegmentsToMove,
        @JsonProperty("replicantLifetime") @Nullable Integer replicantLifetime,
        @JsonProperty("replicationThrottleLimit") @Nullable Integer replicationThrottleLimit,
        @JsonProperty("balancerComputeThreads") @Nullable Integer balancerComputeThreads,
        @JsonProperty("killDataSourceWhitelist") @Nullable Object specificDataSourcesToKillUnusedSegmentsIn,
        @JsonProperty("killTaskSlotRatio") @Nullable Double killTaskSlotRatio,
        @JsonProperty("maxKillTaskSlots") @Nullable Integer maxKillTaskSlots,
        @JsonProperty("killPendingSegmentsSkipList") @Nullable Object dataSourcesToNotKillStalePendingSegmentsIn,
        @JsonProperty("maxSegmentsInNodeLoadingQueue") @Nullable Integer maxSegmentsInNodeLoadingQueue,
        @JsonProperty("decommissioningNodes") @Nullable Object decommissioningNodes,
        @JsonProperty("decommissioningMaxPercentOfMaxSegmentsToMove")
        @Nullable Integer decommissioningMaxPercentOfMaxSegmentsToMove,
        @JsonProperty("pauseCoordination") @Nullable Boolean pauseCoordination,
        @JsonProperty("replicateAfterLoadTimeout") @Nullable Boolean replicateAfterLoadTimeout,
        @JsonProperty("maxNonPrimaryReplicantsToLoad") @Nullable Integer maxNonPrimaryReplicantsToLoad,
        @JsonProperty("useRoundRobinSegmentAssignment") @Nullable Boolean useRoundRobinSegmentAssignment,
        @JsonProperty("smartSegmentLoading") @Nullable Boolean smartSegmentLoading,
        @JsonProperty("debugDimensions") @Nullable Map<String, String> debugDimensions
    )
    {
      this.markSegmentAsUnusedDelayMillis = markSegmentAsUnusedDelayMillis;
      this.mergeBytesLimit = mergeBytesLimit;
      this.mergeSegmentsLimit = mergeSegmentsLimit;
      this.maxSegmentsToMove = maxSegmentsToMove;
      this.replicantLifetime = replicantLifetime;
      this.replicationThrottleLimit = replicationThrottleLimit;
      this.balancerComputeThreads = balancerComputeThreads;
      this.specificDataSourcesToKillUnusedSegmentsIn = specificDataSourcesToKillUnusedSegmentsIn;
      this.killTaskSlotRatio = killTaskSlotRatio;
      this.maxKillTaskSlots = maxKillTaskSlots;
      this.dataSourcesToNotKillStalePendingSegmentsIn = dataSourcesToNotKillStalePendingSegmentsIn;
      this.maxSegmentsInNodeLoadingQueue = maxSegmentsInNodeLoadingQueue;
      this.decommissioningNodes = decommissioningNodes;
      this.decommissioningMaxPercentOfMaxSegmentsToMove = decommissioningMaxPercentOfMaxSegmentsToMove;
      this.pauseCoordination = pauseCoordination;
      this.replicateAfterLoadTimeout = replicateAfterLoadTimeout;
      this.maxNonPrimaryReplicantsToLoad = maxNonPrimaryReplicantsToLoad;
      this.useRoundRobinSegmentAssignment = useRoundRobinSegmentAssignment;
      this.smartSegmentLoading = smartSegmentLoading;
      this.debugDimensions = debugDimensions;
    }

    public Builder withMarkSegmentAsUnusedDelayMillis(long leadingTimeMillis)
    {
      this.markSegmentAsUnusedDelayMillis = leadingTimeMillis;
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

    public Builder withSmartSegmentLoading(boolean smartSegmentLoading)
    {
      this.smartSegmentLoading = smartSegmentLoading;
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

    public Builder withDebugDimensions(Map<String, String> debugDimensions)
    {
      this.debugDimensions = debugDimensions;
      return this;
    }

    public Builder withBalancerComputeThreads(int balancerComputeThreads)
    {
      this.balancerComputeThreads = balancerComputeThreads;
      return this;
    }

    public Builder withSpecificDataSourcesToKillUnusedSegmentsIn(Set<String> dataSources)
    {
      this.specificDataSourcesToKillUnusedSegmentsIn = dataSources;
      return this;
    }

    public Builder withKillTaskSlotRatio(Double killTaskSlotRatio)
    {
      this.killTaskSlotRatio = killTaskSlotRatio;
      return this;
    }

    public Builder withMaxKillTaskSlots(Integer maxKillTaskSlots)
    {
      this.maxKillTaskSlots = maxKillTaskSlots;
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
              markSegmentAsUnusedDelayMillis,
              Defaults.LEADING_MILLIS_BEFORE_MARK_UNUSED
          ),
          valueOrDefault(mergeBytesLimit, Defaults.MERGE_BYTES_LIMIT),
          valueOrDefault(mergeSegmentsLimit, Defaults.MERGE_SEGMENTS_LIMIT),
          valueOrDefault(maxSegmentsToMove, Defaults.MAX_SEGMENTS_TO_MOVE),
          valueOrDefault(replicantLifetime, Defaults.REPLICANT_LIFETIME),
          valueOrDefault(replicationThrottleLimit, Defaults.REPLICATION_THROTTLE_LIMIT),
          valueOrDefault(balancerComputeThreads, getDefaultBalancerComputeThreads()),
          specificDataSourcesToKillUnusedSegmentsIn,
          valueOrDefault(killTaskSlotRatio, Defaults.KILL_TASK_SLOT_RATIO),
          valueOrDefault(maxKillTaskSlots, Defaults.MAX_KILL_TASK_SLOTS),
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
          valueOrDefault(useRoundRobinSegmentAssignment, Defaults.USE_ROUND_ROBIN_ASSIGNMENT),
          valueOrDefault(smartSegmentLoading, Defaults.SMART_SEGMENT_LOADING),
          debugDimensions
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
              markSegmentAsUnusedDelayMillis,
              defaults.getMarkSegmentAsUnusedDelayMillis()
          ),
          valueOrDefault(mergeBytesLimit, defaults.getMergeBytesLimit()),
          valueOrDefault(mergeSegmentsLimit, defaults.getMergeSegmentsLimit()),
          valueOrDefault(maxSegmentsToMove, defaults.getMaxSegmentsToMove()),
          valueOrDefault(replicantLifetime, defaults.getReplicantLifetime()),
          valueOrDefault(replicationThrottleLimit, defaults.getReplicationThrottleLimit()),
          valueOrDefault(balancerComputeThreads, defaults.getBalancerComputeThreads()),
          valueOrDefault(specificDataSourcesToKillUnusedSegmentsIn, defaults.getSpecificDataSourcesToKillUnusedSegmentsIn()),
          valueOrDefault(killTaskSlotRatio, defaults.killTaskSlotRatio),
          valueOrDefault(maxKillTaskSlots, defaults.maxKillTaskSlots),
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
          valueOrDefault(useRoundRobinSegmentAssignment, defaults.isUseRoundRobinSegmentAssignment()),
          valueOrDefault(smartSegmentLoading, defaults.isSmartSegmentLoading()),
          valueOrDefault(debugDimensions, defaults.getDebugDimensions())
      );
    }
  }
}
