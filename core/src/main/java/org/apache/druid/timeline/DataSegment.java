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

package org.apache.druid.timeline;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import com.google.inject.Inject;
import it.unimi.dsi.fastutil.objects.Object2ObjectArrayMap;
import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.jackson.CommaListJoinDeserializer;
import org.apache.druid.jackson.CommaListJoinSerializer;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Metadata of Druid's data segment. An immutable object.
 *
 * DataSegment's equality ({@link #equals}/{@link #hashCode}) and {@link #compareTo} methods consider only the
 * {@link SegmentId} of the segment.
 */
@PublicApi
public class DataSegment implements Comparable<DataSegment>, Overshadowable<DataSegment>
{
  /*
   * The difference between this class and org.apache.druid.segment.Segment is that this class contains the segment
   * metadata only, while org.apache.druid.segment.Segment represents the actual body of segment data, queryable.
   */

  /**
   * This class is needed for optional injection of pruneLoadSpec and pruneLastCompactionState, see
   * github.com/google/guice/wiki/FrequentlyAskedQuestions#how-can-i-inject-optional-parameters-into-a-constructor
   */
  @VisibleForTesting
  public static class PruneSpecsHolder
  {
    @VisibleForTesting
    public static final PruneSpecsHolder DEFAULT = new PruneSpecsHolder();

    @Inject(optional = true) @PruneLoadSpec boolean pruneLoadSpec = false;
    @Inject(optional = true) @PruneLastCompactionState boolean pruneLastCompactionState = false;
  }

  private static final Interner<String> STRING_INTERNER = Interners.newWeakInterner();
  private static final Interner<List<String>> DIMENSIONS_INTERNER = Interners.newWeakInterner();
  private static final Interner<List<String>> METRICS_INTERNER = Interners.newWeakInterner();
  private static final Interner<CompactionState> COMPACTION_STATE_INTERNER = Interners.newWeakInterner();
  private static final Map<String, Object> PRUNED_LOAD_SPEC = ImmutableMap.of(
      "load spec is pruned, because it's not needed on Brokers, but eats a lot of heap space",
      ""
  );

  private final Integer binaryVersion;
  private final SegmentId id;
  @Nullable
  private final Map<String, Object> loadSpec;
  private final List<String> dimensions;
  private final List<String> metrics;
  private final ShardSpec shardSpec;

  /**
   * Stores some configurations of the compaction task which created this segment.
   * This field is filled in the metadata store only when "storeCompactionState" is set true in the context of the
   * compaction task which is false by default.
   * Also, this field can be pruned in many Druid modules when this class is loaded from the metadata store.
   * See {@link PruneLastCompactionState} for details.
   */
  @Nullable
  private final CompactionState lastCompactionState;
  private final long size;

  public DataSegment(
      SegmentId segmentId,
      Map<String, Object> loadSpec,
      List<String> dimensions,
      List<String> metrics,
      ShardSpec shardSpec,
      CompactionState lastCompactionState,
      Integer binaryVersion,
      long size
  )
  {
    this(
        segmentId.getDataSource(),
        segmentId.getInterval(),
        segmentId.getVersion(),
        loadSpec,
        dimensions,
        metrics,
        shardSpec,
        lastCompactionState,
        binaryVersion,
        size
    );
  }

  public DataSegment(
      String dataSource,
      Interval interval,
      String version,
      Map<String, Object> loadSpec,
      List<String> dimensions,
      List<String> metrics,
      ShardSpec shardSpec,
      Integer binaryVersion,
      long size
  )
  {
    this(
        dataSource,
        interval,
        version,
        loadSpec,
        dimensions,
        metrics,
        shardSpec,
        null,
        binaryVersion,
        size
    );
  }

  public DataSegment(
      String dataSource,
      Interval interval,
      String version,
      Map<String, Object> loadSpec,
      List<String> dimensions,
      List<String> metrics,
      ShardSpec shardSpec,
      CompactionState lastCompactionState,
      Integer binaryVersion,
      long size
  )
  {
    this(
        dataSource,
        interval,
        version,
        loadSpec,
        dimensions,
        metrics,
        shardSpec,
        lastCompactionState,
        binaryVersion,
        size,
        PruneSpecsHolder.DEFAULT
    );
  }

  @JsonCreator
  public DataSegment(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("version") String version,
      // use `Map` *NOT* `LoadSpec` because we want to do lazy materialization to prevent dependency pollution
      @JsonProperty("loadSpec") @Nullable Map<String, Object> loadSpec,
      @JsonProperty("dimensions")
      @JsonDeserialize(using = CommaListJoinDeserializer.class)
      @Nullable
          List<String> dimensions,
      @JsonProperty("metrics")
      @JsonDeserialize(using = CommaListJoinDeserializer.class)
      @Nullable
          List<String> metrics,
      @JsonProperty("shardSpec") @Nullable ShardSpec shardSpec,
      @JsonProperty("lastCompactionState") @Nullable CompactionState lastCompactionState,
      @JsonProperty("binaryVersion") Integer binaryVersion,
      @JsonProperty("size") long size,
      @JacksonInject PruneSpecsHolder pruneSpecsHolder
  )
  {
    this.id = SegmentId.of(dataSource, interval, version, shardSpec);
    this.loadSpec = pruneSpecsHolder.pruneLoadSpec ? PRUNED_LOAD_SPEC : prepareLoadSpec(loadSpec);
    // Deduplicating dimensions and metrics lists as a whole because they are very likely the same for the same
    // dataSource
    this.dimensions = prepareDimensionsOrMetrics(dimensions, DIMENSIONS_INTERNER);
    this.metrics = prepareDimensionsOrMetrics(metrics, METRICS_INTERNER);
    this.shardSpec = (shardSpec == null) ? new NumberedShardSpec(0, 1) : shardSpec;
    this.lastCompactionState = pruneSpecsHolder.pruneLastCompactionState
                               ? null
                               : prepareCompactionState(lastCompactionState);
    this.binaryVersion = binaryVersion;
    this.size = size;
  }

  @Nullable
  private Map<String, Object> prepareLoadSpec(@Nullable Map<String, Object> loadSpec)
  {
    if (loadSpec == null) {
      return null;
    }
    // Load spec is just of 3 entries on average; HashMap/LinkedHashMap consumes much more memory than ArrayMap
    Map<String, Object> result = new Object2ObjectArrayMap<>(loadSpec.size());
    for (Map.Entry<String, Object> e : loadSpec.entrySet()) {
      result.put(STRING_INTERNER.intern(e.getKey()), e.getValue());
    }
    return result;
  }

  @Nullable
  private CompactionState prepareCompactionState(@Nullable CompactionState lastCompactionState)
  {
    if (lastCompactionState == null) {
      return null;
    }
    return COMPACTION_STATE_INTERNER.intern(lastCompactionState);
  }

  private List<String> prepareDimensionsOrMetrics(@Nullable List<String> list, Interner<List<String>> interner)
  {
    if (list == null) {
      return ImmutableList.of();
    } else {
      List<String> result = list
          .stream()
          .filter(s -> !Strings.isNullOrEmpty(s))
          // dimensions & metrics are stored as canonical string values to decrease memory required for storing
          // large numbers of segments.
          .map(STRING_INTERNER::intern)
          // TODO replace with ImmutableList.toImmutableList() when updated to Guava 21+
          .collect(Collectors.collectingAndThen(Collectors.toList(), ImmutableList::copyOf));
      return interner.intern(result);
    }
  }

  /**
   * Get dataSource
   *
   * @return the dataSource
   */
  @JsonProperty
  public String getDataSource()
  {
    return id.getDataSource();
  }

  @JsonProperty
  public Interval getInterval()
  {
    return id.getInterval();
  }

  @Nullable
  @JsonProperty
  public Map<String, Object> getLoadSpec()
  {
    return loadSpec;
  }

  @JsonProperty("version")
  @Override
  public String getVersion()
  {
    return id.getVersion();
  }

  @JsonProperty
  @JsonSerialize(using = CommaListJoinSerializer.class)
  public List<String> getDimensions()
  {
    return dimensions;
  }

  @JsonProperty
  @JsonSerialize(using = CommaListJoinSerializer.class)
  public List<String> getMetrics()
  {
    return metrics;
  }

  @JsonProperty
  public ShardSpec getShardSpec()
  {
    return shardSpec;
  }

  @Nullable
  @JsonProperty
  public CompactionState getLastCompactionState()
  {
    return lastCompactionState;
  }

  @JsonProperty
  public Integer getBinaryVersion()
  {
    return binaryVersion;
  }

  @JsonProperty
  public long getSize()
  {
    return size;
  }

  // "identifier" for backward compatibility of JSON API
  @JsonProperty(value = "identifier", access = JsonProperty.Access.READ_ONLY)
  public SegmentId getId()
  {
    return id;
  }

  @Override
  public boolean overshadows(DataSegment other)
  {
    if (id.getDataSource().equals(other.id.getDataSource()) && id.getInterval().overlaps(other.id.getInterval())) {
      final int majorVersionCompare = id.getVersion().compareTo(other.id.getVersion());
      if (majorVersionCompare > 0) {
        return true;
      } else if (majorVersionCompare == 0) {
        return includeRootPartitions(other) && getMinorVersion() > other.getMinorVersion();
      }
    }
    return false;
  }

  @Override
  public int getStartRootPartitionId()
  {
    return shardSpec.getStartRootPartitionId();
  }

  @Override
  public int getEndRootPartitionId()
  {
    return shardSpec.getEndRootPartitionId();
  }

  @Override
  public short getMinorVersion()
  {
    return shardSpec.getMinorVersion();
  }

  @Override
  public short getAtomicUpdateGroupSize()
  {
    return shardSpec.getAtomicUpdateGroupSize();
  }

  private boolean includeRootPartitions(DataSegment other)
  {
    return shardSpec.getStartRootPartitionId() <= other.shardSpec.getStartRootPartitionId()
           && shardSpec.getEndRootPartitionId() >= other.shardSpec.getEndRootPartitionId();
  }

  public SegmentDescriptor toDescriptor()
  {
    return id.toDescriptor();
  }

  public DataSegment withLoadSpec(Map<String, Object> loadSpec)
  {
    return builder(this).loadSpec(loadSpec).build();
  }

  public DataSegment withDimensions(List<String> dimensions)
  {
    return builder(this).dimensions(dimensions).build();
  }

  public DataSegment withMetrics(List<String> metrics)
  {
    return builder(this).metrics(metrics).build();
  }

  public DataSegment withShardSpec(ShardSpec newSpec)
  {
    return builder(this).shardSpec(newSpec).build();
  }

  public DataSegment withSize(long size)
  {
    return builder(this).size(size).build();
  }

  public DataSegment withVersion(String version)
  {
    return builder(this).version(version).build();
  }

  public DataSegment withBinaryVersion(int binaryVersion)
  {
    return builder(this).binaryVersion(binaryVersion).build();
  }

  @Override
  public int compareTo(DataSegment dataSegment)
  {
    return getId().compareTo(dataSegment.getId());
  }

  @Override
  public boolean equals(Object o)
  {
    if (o instanceof DataSegment) {
      return getId().equals(((DataSegment) o).getId());
    }
    return false;
  }

  @Override
  public int hashCode()
  {
    return getId().hashCode();
  }

  @Override
  public String toString()
  {
    return "DataSegment{" +
           "binaryVersion=" + binaryVersion +
           ", id=" + id +
           ", loadSpec=" + loadSpec +
           ", dimensions=" + dimensions +
           ", metrics=" + metrics +
           ", shardSpec=" + shardSpec +
           ", lastCompactionState=" + lastCompactionState +
           ", size=" + size +
           '}';
  }

  public static Builder builder()
  {
    return new Builder();
  }

  public static Builder builder(DataSegment segment)
  {
    return new Builder(segment);
  }

  public static class Builder
  {
    private String dataSource;
    private Interval interval;
    private String version;
    private Map<String, Object> loadSpec;
    private List<String> dimensions;
    private List<String> metrics;
    private ShardSpec shardSpec;
    private CompactionState lastCompactionState;
    private Integer binaryVersion;
    private long size;

    public Builder()
    {
      this.loadSpec = ImmutableMap.of();
      this.dimensions = ImmutableList.of();
      this.metrics = ImmutableList.of();
      this.shardSpec = new NumberedShardSpec(0, 1);
      this.size = -1;
    }

    public Builder(DataSegment segment)
    {
      this.dataSource = segment.getDataSource();
      this.interval = segment.getInterval();
      this.version = segment.getVersion();
      this.loadSpec = segment.getLoadSpec();
      this.dimensions = segment.getDimensions();
      this.metrics = segment.getMetrics();
      this.shardSpec = segment.getShardSpec();
      this.lastCompactionState = segment.getLastCompactionState();
      this.binaryVersion = segment.getBinaryVersion();
      this.size = segment.getSize();
    }

    public Builder dataSource(String dataSource)
    {
      this.dataSource = dataSource;
      return this;
    }

    public Builder interval(Interval interval)
    {
      this.interval = interval;
      return this;
    }

    public Builder version(String version)
    {
      this.version = version;
      return this;
    }

    public Builder loadSpec(Map<String, Object> loadSpec)
    {
      this.loadSpec = loadSpec;
      return this;
    }

    public Builder dimensions(List<String> dimensions)
    {
      this.dimensions = dimensions;
      return this;
    }

    public Builder metrics(List<String> metrics)
    {
      this.metrics = metrics;
      return this;
    }

    public Builder shardSpec(ShardSpec shardSpec)
    {
      this.shardSpec = shardSpec;
      return this;
    }

    public Builder lastCompactionState(CompactionState compactionState)
    {
      this.lastCompactionState = compactionState;
      return this;
    }

    public Builder binaryVersion(Integer binaryVersion)
    {
      this.binaryVersion = binaryVersion;
      return this;
    }

    public Builder size(long size)
    {
      this.size = size;
      return this;
    }

    public DataSegment build()
    {
      // Check stuff that goes into the id, at least.
      Preconditions.checkNotNull(dataSource, "dataSource");
      Preconditions.checkNotNull(interval, "interval");
      Preconditions.checkNotNull(version, "version");
      Preconditions.checkNotNull(shardSpec, "shardSpec");

      return new DataSegment(
          dataSource,
          interval,
          version,
          loadSpec,
          dimensions,
          metrics,
          shardSpec,
          lastCompactionState,
          binaryVersion,
          size
      );
    }
  }
}
