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

package io.druid.timeline;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import com.google.common.collect.Iterables;
import io.druid.guice.annotations.PublicApi;
import io.druid.jackson.CommaListJoinDeserializer;
import io.druid.jackson.CommaListJoinSerializer;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.query.SegmentDescriptor;
import io.druid.timeline.partition.NoneShardSpec;
import io.druid.timeline.partition.ShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 */
@PublicApi
public class DataSegment implements Comparable<DataSegment>
{
  public static String delimiter = "_";
  private final Integer binaryVersion;
  private static final Interner<String> interner = Interners.newWeakInterner();
  private static final Function<String, String> internFun = new Function<String, String>()
  {
    @Override
    public String apply(String input)
    {
      return interner.intern(input);
    }
  };

  public static String makeDataSegmentIdentifier(
      String dataSource,
      DateTime start,
      DateTime end,
      String version,
      ShardSpec shardSpec
  )
  {
    StringBuilder sb = new StringBuilder();

    sb.append(dataSource).append(delimiter)
      .append(start).append(delimiter)
      .append(end).append(delimiter)
      .append(version);

    if (shardSpec.getPartitionNum() != 0) {
      sb.append(delimiter).append(shardSpec.getPartitionNum());
    }

    return sb.toString();
  }

  private final String dataSource;
  private final Interval interval;
  private final String version;
  private final Map<String, Object> loadSpec;
  private final List<String> dimensions;
  private final List<String> metrics;
  private final ShardSpec shardSpec;
  private final long size;
  private final String identifier;

  @JsonCreator
  public DataSegment(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("version") String version,
      // use `Map` *NOT* `LoadSpec` because we want to do lazy materialization to prevent dependency pollution
      @JsonProperty("loadSpec") Map<String, Object> loadSpec,
      @JsonProperty("dimensions") @JsonDeserialize(using = CommaListJoinDeserializer.class) List<String> dimensions,
      @JsonProperty("metrics") @JsonDeserialize(using = CommaListJoinDeserializer.class) List<String> metrics,
      @JsonProperty("shardSpec") ShardSpec shardSpec,
      @JsonProperty("binaryVersion") Integer binaryVersion,
      @JsonProperty("size") long size
  )
  {
    final Predicate<String> nonEmpty = new Predicate<String>()
    {
      @Override
      public boolean apply(String input)
      {
        return input != null && !input.isEmpty();
      }
    };

    // dataSource, dimensions & metrics are stored as canonical string values to decrease memory required for storing large numbers of segments.
    this.dataSource = interner.intern(dataSource);
    this.interval = interval;
    this.loadSpec = loadSpec;
    this.version = version;
    this.dimensions = dimensions == null
                      ? ImmutableList.<String>of()
                      : ImmutableList.copyOf(Iterables.transform(Iterables.filter(dimensions, nonEmpty), internFun));
    this.metrics = metrics == null
                   ? ImmutableList.<String>of()
                   : ImmutableList.copyOf(Iterables.transform(Iterables.filter(metrics, nonEmpty), internFun));
    this.shardSpec = (shardSpec == null) ? NoneShardSpec.instance() : shardSpec;
    this.binaryVersion = binaryVersion;
    this.size = size;

    this.identifier = makeDataSegmentIdentifier(
        this.dataSource,
        this.interval.getStart(),
        this.interval.getEnd(),
        this.version,
        this.shardSpec
    );
  }

  /**
   * Get dataSource
   *
   * @return the dataSource
   */
  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public Interval getInterval()
  {
    return interval;
  }

  @JsonProperty
  public Map<String, Object> getLoadSpec()
  {
    return loadSpec;
  }

  @JsonProperty
  public String getVersion()
  {
    return version;
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

  @JsonProperty
  public String getIdentifier()
  {
    return identifier;
  }

  public SegmentDescriptor toDescriptor()
  {
    return new SegmentDescriptor(interval, version, shardSpec.getPartitionNum());
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
    return getIdentifier().compareTo(dataSegment.getIdentifier());
  }

  @Override
  public boolean equals(Object o)
  {
    if (o instanceof DataSegment) {
      return getIdentifier().equals(((DataSegment) o).getIdentifier());
    }
    return false;
  }

  @Override
  public int hashCode()
  {
    return getIdentifier().hashCode();
  }

  @Override
  public String toString()
  {
    return "DataSegment{" +
           "size=" + size +
           ", shardSpec=" + shardSpec +
           ", metrics=" + metrics +
           ", dimensions=" + dimensions +
           ", version='" + version + '\'' +
           ", loadSpec=" + loadSpec +
           ", interval=" + interval +
           ", dataSource='" + dataSource + '\'' +
           ", binaryVersion='" + binaryVersion + '\'' +
           '}';
  }

  public static Comparator<DataSegment> bucketMonthComparator()
  {
    return new Comparator<DataSegment>()
    {
      @Override
      public int compare(DataSegment lhs, DataSegment rhs)
      {
        int retVal;

        DateTime lhsMonth = Granularities.MONTH.bucketStart(lhs.getInterval().getStart());
        DateTime rhsMonth = Granularities.MONTH.bucketStart(rhs.getInterval().getStart());

        retVal = lhsMonth.compareTo(rhsMonth);

        if (retVal != 0) {
          return retVal;
        }

        return lhs.compareTo(rhs);
      }
    };
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
    private Integer binaryVersion;
    private long size;

    public Builder()
    {
      this.loadSpec = ImmutableMap.of();
      this.dimensions = ImmutableList.of();
      this.metrics = ImmutableList.of();
      this.shardSpec = NoneShardSpec.instance();
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
      // Check stuff that goes into the identifier, at least.
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
          binaryVersion,
          size
      );
    }
  }
}
