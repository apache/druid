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
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.msq.input.LoadableSegment;
import org.apache.druid.msq.input.PhysicalInputSlice;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.filter.DimFilter;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Input spec representing a Druid table.
 */
@JsonTypeName("table")
public class TableInputSpec implements InputSpec
{
  private final String dataSource;
  private final List<Interval> intervals;

  @Nullable
  private final List<SegmentDescriptor> segments;

  @Nullable
  private final DimFilter filter;

  @Nullable
  private final Set<String> filterFields;

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
   * @param filter       other filters to use for pruning, or null if no pruning is desired. Pruning filters are
   *                     *not strict*, which means that processors must re-apply them when processing the returned
   *                     {@link LoadableSegment} from {@link PhysicalInputSlice#getLoadableSegments()}. This matches how
   *                     Broker-based pruning works for native queries.
   * @param filterFields list of fields from {@link DimFilter#getRequiredColumns()} to consider for pruning. If null,
   *                     all fields are considered for pruning.
   */
  @JsonCreator
  public TableInputSpec(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("intervals") @Nullable List<Interval> intervals,
      @JsonProperty("segments") @Nullable List<SegmentDescriptor> segments,
      @JsonProperty("filter") @Nullable DimFilter filter,
      @JsonProperty("filterFields") @Nullable Set<String> filterFields
  )
  {
    this.dataSource = dataSource;
    this.intervals = intervals == null ? Intervals.ONLY_ETERNITY : intervals;
    if (segments != null && segments.isEmpty()) {
      throw DruidException.defensive(
          "Can not supply empty segments as input, please use either null or non-empty segments.");
    }
    this.segments = segments;
    this.filter = filter;
    this.filterFields = filterFields;
  }

  /**
   * @deprecated Use {@link #TableInputSpec(String, List, List, DimFilter, Set)} with explicit null for segments instead.
   */
  @Deprecated
  public TableInputSpec(
      String dataSource,
      @Nullable List<Interval> intervals,
      @Nullable DimFilter filter,
      @Nullable Set<String> filterFields
  )
  {
    this(dataSource, intervals, null, filter, filterFields);
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
  public DimFilter getFilter()
  {
    return filter;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  public Set<String> getFilterFields()
  {
    return filterFields;
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
           && Objects.equals(filter, that.filter)
           && Objects.equals(filterFields, that.filterFields);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dataSource, intervals, segments, filter, filterFields);
  }

  @Override
  public String toString()
  {
    return "TableInputSpec{" +
           "dataSource='" + dataSource + '\'' +
           ", intervals=" + intervals +
           (segments == null ? "" : ", segments=" + segments) +
           (filter == null ? "" : ", filter=" + filter) +
           (filterFields == null ? "" : ", filterFields=" + filterFields) +
           '}';
  }
}
