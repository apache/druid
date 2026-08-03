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

package org.apache.druid.query.metadata.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.AggregateProjectionMetadata;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class SegmentAnalysis implements Comparable<SegmentAnalysis>
{
  /**
   * Segment id is stored as a String rather than {@link org.apache.druid.timeline.SegmentId}, because when a
   * SegmentAnalysis object is sent across Druid nodes, on the receiver (deserialization) side it's impossible to
   * unambiguously convert a segment id string (as transmitted in the JSON format) back into a {@code SegmentId} object
   * ({@link org.apache.druid.timeline.SegmentId#tryParse} javadoc explains that ambiguities in details). It would be
   * fine to have the type of this field of Object, setting it to {@code SegmentId} on the sender side and remaining as
   * a String on the receiver side, but it's even less type-safe than always storing the segment id as a String.
   */
  private final String id;
  private final List<Interval> interval;

  /**
   * Require LinkedHashMap to emphasize how important column order is. It's used by DruidSchema to keep
   * SQL column order in line with ingestion column order.
   */
  private final LinkedHashMap<String, ColumnAnalysis> columns;
  private final long size;
  private final long numRows;
  private final Map<String, AggregatorFactory> aggregators;
  private final Map<String, AggregateProjectionMetadata> projections;
  private final TimestampSpec timestampSpec;
  private final Granularity queryGranularity;
  private final Boolean rollup;
  private final List<ContainerAnalysis> containers;

  /**
   * Retained for binary compatibility with code compiled against the pre-{@link ContainerAnalysis} constructor.
   * New callers should use {@link Builder}.
   */
  @Deprecated
  public SegmentAnalysis(
      String id,
      List<Interval> interval,
      LinkedHashMap<String, ColumnAnalysis> columns,
      long size,
      long numRows,
      Map<String, AggregatorFactory> aggregators,
      Map<String, AggregateProjectionMetadata> projections,
      TimestampSpec timestampSpec,
      Granularity queryGranularity,
      Boolean rollup
  )
  {
    this(id, interval, columns, size, numRows, aggregators, projections, timestampSpec, queryGranularity, rollup, null);
  }

  @JsonCreator
  SegmentAnalysis(
      @JsonProperty("id") String id,
      @JsonProperty("intervals") List<Interval> interval,
      @JsonProperty("columns") LinkedHashMap<String, ColumnAnalysis> columns,
      @JsonProperty("size") long size,
      @JsonProperty("numRows") long numRows,
      @JsonProperty("aggregators") Map<String, AggregatorFactory> aggregators,
      @JsonProperty("projections") Map<String, AggregateProjectionMetadata> projections,
      @JsonProperty("timestampSpec") TimestampSpec timestampSpec,
      @JsonProperty("queryGranularity") Granularity queryGranularity,
      @JsonProperty("rollup") Boolean rollup,
      @JsonProperty("containers") List<ContainerAnalysis> containers
  )
  {
    this.id = id;
    this.interval = interval;
    this.columns = columns;
    this.size = size;
    this.numRows = numRows;
    this.aggregators = aggregators;
    this.projections = projections;
    this.timestampSpec = timestampSpec;
    this.queryGranularity = queryGranularity;
    this.rollup = rollup;
    this.containers = containers;
  }

  @JsonProperty
  public String getId()
  {
    return id;
  }

  @JsonProperty
  public List<Interval> getIntervals()
  {
    return interval;
  }

  @JsonProperty
  public LinkedHashMap<String, ColumnAnalysis> getColumns()
  {
    return columns;
  }

  @JsonProperty
  public long getSize()
  {
    return size;
  }

  @JsonProperty
  public long getNumRows()
  {
    return numRows;
  }

  @JsonProperty
  public TimestampSpec getTimestampSpec()
  {
    return timestampSpec;
  }

  @JsonProperty
  public Granularity getQueryGranularity()
  {
    return queryGranularity;
  }

  @JsonProperty
  public Boolean isRollup()
  {
    return rollup;
  }

  @JsonProperty
  public Map<String, AggregatorFactory> getAggregators()
  {
    return aggregators;
  }

  @JsonProperty
  public Map<String, AggregateProjectionMetadata> getProjections()
  {
    return projections;
  }

  @JsonProperty
  public List<ContainerAnalysis> getContainers()
  {
    return containers;
  }

  @Override
  public String toString()
  {
    return "SegmentAnalysis{" +
           "id='" + id + '\'' +
           ", interval=" + interval +
           ", columns=" + columns +
           ", size=" + size +
           ", numRows=" + numRows +
           ", aggregators=" + aggregators +
           ", projections=" + projections +
           ", timestampSpec=" + timestampSpec +
           ", queryGranularity=" + queryGranularity +
           ", rollup=" + rollup +
           ", containers=" + containers +
           '}';
  }

  /**
   * Best-effort equals method; relies on AggregatorFactory.equals, which is not guaranteed to be sanely implemented.
   */
  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SegmentAnalysis that = (SegmentAnalysis) o;
    return size == that.size &&
           numRows == that.numRows &&
           Objects.equals(rollup, that.rollup) &&
           Objects.equals(id, that.id) &&
           Objects.equals(interval, that.interval) &&
           Objects.equals(columns, that.columns) &&
           Objects.equals(aggregators, that.aggregators) &&
           Objects.equals(projections, that.projections) &&
           Objects.equals(timestampSpec, that.timestampSpec) &&
           Objects.equals(queryGranularity, that.queryGranularity) &&
           Objects.equals(containers, that.containers);
  }

  /**
   * Best-effort hashCode method; relies on AggregatorFactory.hashCode, which is not guaranteed to be sanely
   * implemented.
   */
  @Override
  public int hashCode()
  {
    return Objects.hash(
        id,
        interval,
        columns,
        size,
        numRows,
        aggregators,
        projections,
        timestampSpec,
        queryGranularity,
        rollup,
        containers
    );
  }

  @Override
  public int compareTo(SegmentAnalysis rhs)
  {
    return id.compareTo(rhs.getId());
  }

  /**
   * Helper class to build {@link SegmentAnalysis} objects. Supports both incremental, single-entry building (handy
   * for tests) and bulk setters that take an already-computed map/list (handy for production call sites that
   * already have the whole thing on hand).
   */
  public static class Builder
  {
    private final String segmentId;
    private LinkedHashMap<String, ColumnAnalysis> columns = new LinkedHashMap<>();
    private Map<String, AggregatorFactory> aggregators = new LinkedHashMap<>();
    private Map<String, AggregateProjectionMetadata> projections = new LinkedHashMap<>();
    private List<ContainerAnalysis> containers = new ArrayList<>();

    private List<Interval> intervals = null;
    private Optional<Long> size = Optional.empty();
    private Optional<Long> numRows = Optional.empty();
    private Optional<Boolean> rollup = Optional.empty();
    @Nullable
    private TimestampSpec timestampSpec = null;
    @Nullable
    private Granularity queryGranularity = null;

    public Builder(String segmentId)
    {
      this.segmentId = segmentId;
    }

    public Builder(SegmentId segmentId)
    {
      this.segmentId = segmentId.toString();
    }

    public Builder size(long size)
    {
      if (this.size.isEmpty()) {
        this.size = Optional.of(size);
      } else {
        throw new IllegalStateException("Size is already set: " + this.size.get());
      }
      return this;
    }

    /**
     * Retained for binary compatibility with code compiled against the pre-{@link ContainerAnalysis} signature.
     */
    @Deprecated
    public Builder size(int size)
    {
      return size((long) size);
    }

    public Builder numRows(long numRows)
    {
      if (this.numRows.isEmpty()) {
        this.numRows = Optional.of(numRows);
      } else {
        throw new IllegalStateException("NumRows is already set: " + this.numRows.get());
      }
      return this;
    }

    /**
     * Retained for binary compatibility with code compiled against the pre-{@link ContainerAnalysis} signature.
     */
    @Deprecated
    public Builder numRows(int numRows)
    {
      return numRows((long) numRows);
    }

    public Builder rollup(@Nullable Boolean rollup)
    {
      if (rollup == null) {
        return this;
      }
      if (this.rollup.isEmpty()) {
        this.rollup = Optional.of(rollup);
      } else {
        throw new IllegalStateException("Rollup is already set: " + this.rollup.get());
      }
      return this;
    }

    /**
     * Retained for binary compatibility with code compiled against the pre-{@link ContainerAnalysis} signature.
     */
    @Deprecated
    public Builder rollup(boolean rollup)
    {
      return rollup(Boolean.valueOf(rollup));
    }

    public Builder interval(Interval interval)
    {
      if (this.intervals == null) {
        this.intervals = new ArrayList<>();
      }
      this.intervals.add(interval);
      return this;
    }

    public Builder intervals(@Nullable List<Interval> intervals)
    {
      this.intervals = intervals == null ? null : new ArrayList<>(intervals);
      return this;
    }

    public Builder column(String columnName, ColumnAnalysis columnAnalysis)
    {
      this.columns.put(columnName, columnAnalysis);
      return this;
    }

    public Builder columns(@Nullable LinkedHashMap<String, ColumnAnalysis> columns)
    {
      this.columns = columns == null ? new LinkedHashMap<>() : new LinkedHashMap<>(columns);
      return this;
    }

    public Builder aggregator(String name, AggregatorFactory aggregatorFactory)
    {
      this.aggregators.put(name, aggregatorFactory);
      return this;
    }

    public Builder aggregators(@Nullable Map<String, AggregatorFactory> aggregators)
    {
      this.aggregators = aggregators == null ? new LinkedHashMap<>() : new LinkedHashMap<>(aggregators);
      return this;
    }

    public Builder projection(String name, AggregateProjectionMetadata projection)
    {
      this.projections.put(name, projection);
      return this;
    }

    public Builder projections(@Nullable Map<String, AggregateProjectionMetadata> projections)
    {
      this.projections = projections == null ? new LinkedHashMap<>() : new LinkedHashMap<>(projections);
      return this;
    }

    public Builder container(String bundle, long size)
    {
      this.containers.add(new ContainerAnalysis(bundle, size));
      return this;
    }

    public Builder containers(@Nullable List<ContainerAnalysis> containers)
    {
      this.containers = containers == null ? new ArrayList<>() : new ArrayList<>(containers);
      return this;
    }

    public Builder timestampSpec(@Nullable TimestampSpec timestampSpec)
    {
      this.timestampSpec = timestampSpec;
      return this;
    }

    public Builder queryGranularity(@Nullable Granularity queryGranularity)
    {
      this.queryGranularity = queryGranularity;
      return this;
    }

    public SegmentAnalysis build()
    {
      return new SegmentAnalysis(
          segmentId,
          intervals,
          columns,
          size.orElse(0L),
          numRows.orElse(0L),
          aggregators.isEmpty() ? null : aggregators,
          projections.isEmpty() ? null : projections,
          timestampSpec,
          queryGranularity,
          rollup.orElse(null),
          containers.isEmpty() ? null : containers
      );
    }
  }

  /**
   * On-disk byte size of a single segment file container (a V10 file format bundle), reported by
   * {@link SegmentMetadataQuery.AnalysisType#CONTAINERSIZE}. One entry per physical container; a bundle (e.g. a
   * projection name) may have more than one container if its contents exceeded the writer's max container size.
   *
   * @param bundle owning bundle name (e.g. the base table or a projection's name)
   * @param size   on-disk byte size of this container
   */
  public record ContainerAnalysis(
      @JsonProperty("bundle") String bundle,
      @JsonProperty("size") long size
  )
  {
  }
}
