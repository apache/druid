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

package org.apache.druid.msq.input.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.msq.input.LoadableSegment;
import org.apache.druid.msq.input.PhysicalInputSlice;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.FilterSegmentPruner;
import org.apache.druid.query.filter.SegmentPruner;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Input spec representing a Druid table.
 */
@JsonTypeName("table")
public class TableInputSpec implements InputSpec
{
  public static TableInputSpec fullScan(String datasource)
  {
    return new TableInputSpec(datasource,null, null, null);
  }

  private final String dataSource;
  private final List<Interval> intervals;

  @Nullable
  private final List<SegmentDescriptor> segments;

  @Nullable
  private final SegmentPruner pruner;

  /**
   * Create a table input spec.
   *
   * @param dataSource   datasource to read
   * @param intervals    intervals to filter, or null if no time filtering is desired. Interval filtering is strict,
   *                     meaning that when this spec is sliced and read, the returned {@link LoadableSegment}
   *                     from {@link PhysicalInputSlice#getLoadableSegments()} are clipped to these intervals using
   *                     {@link LoadableSegment#descriptor()}.
   * @param segments     specific segments to read, or null to read all segments in the intervals. If provided,
   *                     only these segments will be read. Must not be empty if non-null.
   * @param pruner       additional segment purning, or null if no pruning is desired. Pruning done in this manner is
   *                     *not strict*, which means that processors must re-apply them when processing the returned
   *                     {@link LoadableSegment} from {@link PhysicalInputSlice#getLoadableSegments()}. This matches how
   *                     Broker-based pruning works for native queries.
   * @param filter       *deprecated* legacy form of pruner functionality
   * @param filterFields *deprecated* legacy form of pruner functionality
   */
  @JsonCreator
  public TableInputSpec(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("intervals") @Nullable List<Interval> intervals,
      @JsonProperty("segments") @Nullable List<SegmentDescriptor> segments,
      @JsonProperty("pruner") @Nullable SegmentPruner pruner,
      @JsonProperty("filter") @Deprecated @Nullable DimFilter filter,
      @JsonProperty("filterFields") @Deprecated @Nullable Set<String> filterFields
  )
  {
    this.dataSource = dataSource;
    this.intervals = intervals == null ? Intervals.ONLY_ETERNITY : intervals;
    if (segments != null && segments.isEmpty()) {
      throw new IAE("Can not supply empty segments as input, please use either null or non-empty segments.");
    }
    this.segments = segments;
    // pruner might be null, check for deprecated fields
    if (pruner != null) {
      this.pruner = pruner;
    } else if (filter != null) {
      this.pruner = new FilterSegmentPruner(
          filter,
          filterFields
      );
    } else {
      this.pruner = null;
    }
  }

  public TableInputSpec(
      String dataSource,
      @Nullable List<Interval> intervals,
      @Nullable List<SegmentDescriptor> segments,
      @Nullable SegmentPruner pruner
  )
  {
    this(dataSource, intervals, segments, pruner, null, null);
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public List<Interval> getIntervals()
  {
    return intervals;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  private List<Interval> getIntervalsForSerialization()
  {
    return intervals.equals(Intervals.ONLY_ETERNITY) ? null : intervals;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  public List<SegmentDescriptor> getSegments()
  {
    return segments;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  public SegmentPruner getPruner()
  {
    return pruner;
  }

  /**
   * @deprecated delete sometime after Druid 37
   */
  @Deprecated
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  public DimFilter getFilter()
  {
    // serialized for backwards compatibility
    if (pruner instanceof FilterSegmentPruner) {
      return ((FilterSegmentPruner) pruner).getFilter();
    }
    return null;
  }

  /**
   * @deprecated delete sometime after Druid 37
   */
  @Deprecated
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  public Set<String> getFilterFields()
  {
    if (pruner instanceof FilterSegmentPruner) {
      return ((FilterSegmentPruner) pruner).getFilterFields();
    }
    return null;
  }

  public <T> Collection<T> filterSegments(final Iterable<T> input, final Function<T, DataSegment> converter)
  {
    if (pruner == null) {
      return ImmutableSet.copyOf(input);
    }
    return pruner.prune(
        input,
        converter
    );
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
    TableInputSpec that = (TableInputSpec) o;
    return Objects.equals(dataSource, that.dataSource)
           && Objects.equals(intervals, that.intervals)
           && Objects.equals(segments, that.segments)
           && Objects.equals(pruner, that.pruner);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dataSource, intervals, segments, pruner);
  }

  @Override
  public String toString()
  {
    return "TableInputSpec{" +
           "dataSource='" + dataSource + '\'' +
           ", intervals=" + intervals +
           (segments == null ? "" : ", segments=" + segments) +
           (pruner == null ? "" : ", pruner=" + pruner) +
           '}';
  }
}
