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
package io.druid.server.coordinator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import io.druid.java.util.common.IAE;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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
  private final Set<String> killDataSourceWhitelist;

  // The pending segments of the dataSources in this list are not killed.
  private final Set<String> killPendingSegmentsSkipList;

  /**
   * The maximum number of segments that could be queued for loading to any given server.
   * Default values is 0 with the meaning of "unbounded" (any number of
   * segments could be added to the loading queue for any server).
   * See {@link LoadQueuePeon}, {@link io.druid.server.coordinator.rules.LoadRule#run}
   */
  private final int maxSegmentsInNodeLoadingQueue;
  private final List<CoordinatorCompactionConfig> compactionConfigs;
  private final double compactionTaskSlotRatio;
  private final int maxCompactionTaskSlots;

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
      // See https://github.com/druid-io/druid/issues/3055
      @JsonProperty("killDataSourceWhitelist") Object killDataSourceWhitelist,
      @JsonProperty("killAllDataSources") boolean killAllDataSources,
      @JsonProperty("killPendingSegmentsSkipList") Object killPendingSegmentsSkipList,
      @JsonProperty("maxSegmentsInNodeLoadingQueue") int maxSegmentsInNodeLoadingQueue,
      @JsonProperty("compactionConfigs") List<CoordinatorCompactionConfig> compactionConfigs,
      @JsonProperty("compactionTaskSlotRatio") double compactionTaskSlotRatio,
      @JsonProperty("maxCompactionTaskSlots") int maxCompactionTaskSlots
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
    this.killDataSourceWhitelist = parseJsonStringOrArray(killDataSourceWhitelist);
    this.killPendingSegmentsSkipList = parseJsonStringOrArray(killPendingSegmentsSkipList);
    this.maxSegmentsInNodeLoadingQueue = maxSegmentsInNodeLoadingQueue;
    this.compactionConfigs = compactionConfigs;
    this.compactionTaskSlotRatio = compactionTaskSlotRatio;
    this.maxCompactionTaskSlots = maxCompactionTaskSlots;

    if (this.killAllDataSources && !this.killDataSourceWhitelist.isEmpty()) {
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

  @JsonProperty
  public Set<String> getKillDataSourceWhitelist()
  {
    return killDataSourceWhitelist;
  }

  @JsonProperty
  public boolean isKillAllDataSources()
  {
    return killAllDataSources;
  }

  @JsonProperty
  public Set<String> getKillPendingSegmentsSkipList()
  {
    return killPendingSegmentsSkipList;
  }

  @JsonProperty
  public int getMaxSegmentsInNodeLoadingQueue()
  {
    return maxSegmentsInNodeLoadingQueue;
  }

  @JsonProperty
  public List<CoordinatorCompactionConfig> getCompactionConfigs()
  {
    return compactionConfigs;
  }

  @JsonProperty
  public double getCompactionTaskSlotRatio()
  {
    return compactionTaskSlotRatio;
  }

  @JsonProperty
  public int getMaxCompactionTaskSlots()
  {
    return maxCompactionTaskSlots;
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
           ", killDataSourceWhitelist=" + killDataSourceWhitelist +
           ", killAllDataSources=" + killAllDataSources +
           ", killPendingSegmentsSkipList=" + killPendingSegmentsSkipList +
           ", maxSegmentsInNodeLoadingQueue=" + maxSegmentsInNodeLoadingQueue +
           ", compactionConfigs=" + compactionConfigs +
           ", compactionTaskSlotRatio=" + compactionTaskSlotRatio +
           ", maxCompactionTaskSlots=" + maxCompactionTaskSlots +
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
    if (!Objects.equals(killDataSourceWhitelist, that.killDataSourceWhitelist)) {
      return false;
    }
    if (!Objects.equals(killPendingSegmentsSkipList, that.killPendingSegmentsSkipList)) {
      return false;
    }
    if (!Objects.equals(compactionConfigs, that.compactionConfigs)) {
      return false;
    }
    if (compactionTaskSlotRatio != that.compactionTaskSlotRatio) {
      return false;
    }

    return maxCompactionTaskSlots == that.maxCompactionTaskSlots;
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
        killDataSourceWhitelist,
        killPendingSegmentsSkipList,
        compactionConfigs,
        compactionTaskSlotRatio,
        maxCompactionTaskSlots
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
    private static final double DEFAULT_COMPACTION_TASK_RATIO = 0.1;
    private static final int DEFAILT_MAX_COMPACTION_TASK_SLOTS = Integer.MAX_VALUE;

    private Long millisToWaitBeforeDeleting;
    private Long mergeBytesLimit;
    private Integer mergeSegmentsLimit;
    private Integer maxSegmentsToMove;
    private Integer replicantLifetime;
    private Integer replicationThrottleLimit;
    private Boolean emitBalancingStats;
    private Integer balancerComputeThreads;
    private Object killDataSourceWhitelist;
    private Boolean killAllDataSources;
    private Object killPendingSegmentsSkipList;
    private Integer maxSegmentsInNodeLoadingQueue;
    private List<CoordinatorCompactionConfig> compactionConfigs;
    private Double compactionTaskRatio;
    private Integer maxCompactionTaskSlots;

    public Builder()
    {
    }

    @JsonCreator
    public Builder(
        @JsonProperty("millisToWaitBeforeDeleting") Long millisToWaitBeforeDeleting,
        @JsonProperty("mergeBytesLimit") Long mergeBytesLimit,
        @JsonProperty("mergeSegmentsLimit") Integer mergeSegmentsLimit,
        @JsonProperty("maxSegmentsToMove") Integer maxSegmentsToMove,
        @JsonProperty("replicantLifetime") Integer replicantLifetime,
        @JsonProperty("replicationThrottleLimit") Integer replicationThrottleLimit,
        @JsonProperty("balancerComputeThreads") Integer balancerComputeThreads,
        @JsonProperty("emitBalancingStats") Boolean emitBalancingStats,
        @JsonProperty("killDataSourceWhitelist") Object killDataSourceWhitelist,
        @JsonProperty("killAllDataSources") Boolean killAllDataSources,
        @JsonProperty("killPendingSegmentsSkipList") Object killPendingSegmentsSkipList,
        @JsonProperty("maxSegmentsInNodeLoadingQueue") Integer maxSegmentsInNodeLoadingQueue,
        @JsonProperty("compactionConfigs") List<CoordinatorCompactionConfig> compactionConfigs,
        @JsonProperty("compactionTaskSlotRatio") Double compactionTaskRatio,
        @JsonProperty("maxCompactionTaskSlots") Integer maxCompactionTaskSlots
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
      this.killDataSourceWhitelist = killDataSourceWhitelist;
      this.killPendingSegmentsSkipList = killPendingSegmentsSkipList;
      this.maxSegmentsInNodeLoadingQueue = maxSegmentsInNodeLoadingQueue;
      this.compactionConfigs = compactionConfigs;
      this.compactionTaskRatio = compactionTaskRatio;
      this.maxCompactionTaskSlots = maxCompactionTaskSlots;
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
      this.killDataSourceWhitelist = killDataSourceWhitelist;
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

    public Builder withCompactionConfigs(List<CoordinatorCompactionConfig> compactionConfigs)
    {
      this.compactionConfigs = compactionConfigs;
      return this;
    }

    public Builder withCompactionTaskRatio(double compactionTaskRatio)
    {
      this.compactionTaskRatio = compactionTaskRatio;
      return this;
    }

    public Builder withMaxCompactionTaskSlots(int maxCompactionTaskSlots)
    {
      this.maxCompactionTaskSlots = maxCompactionTaskSlots;
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
          killDataSourceWhitelist,
          killAllDataSources == null ? DEFAULT_KILL_ALL_DATA_SOURCES : killAllDataSources,
          killPendingSegmentsSkipList,
          maxSegmentsInNodeLoadingQueue == null ? DEFAULT_MAX_SEGMENTS_IN_NODE_LOADING_QUEUE : maxSegmentsInNodeLoadingQueue,
          compactionConfigs,
          compactionTaskRatio == null ? DEFAULT_COMPACTION_TASK_RATIO : compactionTaskRatio,
          maxCompactionTaskSlots == null ? DEFAILT_MAX_COMPACTION_TASK_SLOTS : maxCompactionTaskSlots
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
          killDataSourceWhitelist == null ? defaults.getKillDataSourceWhitelist() : killDataSourceWhitelist,
          killAllDataSources == null ? defaults.isKillAllDataSources() : killAllDataSources,
          killPendingSegmentsSkipList == null ? defaults.getKillPendingSegmentsSkipList() : killPendingSegmentsSkipList,
          maxSegmentsInNodeLoadingQueue == null ? defaults.getMaxSegmentsInNodeLoadingQueue() : maxSegmentsInNodeLoadingQueue,
          compactionConfigs == null ? defaults.getCompactionConfigs() : compactionConfigs,
          compactionTaskRatio == null ? defaults.getCompactionTaskSlotRatio() : compactionTaskRatio,
          maxCompactionTaskSlots == null ? defaults.getMaxCompactionTaskSlots() : maxCompactionTaskSlots
      );
    }
  }
}
