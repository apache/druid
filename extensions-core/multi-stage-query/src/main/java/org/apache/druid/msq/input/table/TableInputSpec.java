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
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.msq.input.ReadableInput;
import org.apache.druid.query.filter.DimFilter;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * Input spec representing a Druid table.
 */
@JsonTypeName("table")
public class TableInputSpec implements InputSpec
{
  private final String dataSource;
  private final List<Interval> intervals;

  @Nullable
  private final DimFilter filter;

  /**
   * Create a table input spec.
   *
   * @param dataSource datasource to read
   * @param intervals  intervals to filter, or null if no time filtering is desired. Interval filtering is strict,
   *                   meaning that when this spec is sliced and read, the returned {@link SegmentWithDescriptor}
   *                   from {@link ReadableInput#getSegment()} are clipped to these intervals.
   * @param filter     other filters to use for pruning, or null if no pruning is desired. Pruning filters are
   *                   *not strict*, which means that processors must re-apply them when processing the returned
   *                   {@link SegmentWithDescriptor} from {@link ReadableInput#getSegment()}. This matches how
   *                   Broker-based pruning works for native queries.
   */
  @JsonCreator
  public TableInputSpec(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("intervals") @Nullable List<Interval> intervals,
      @JsonProperty("filter") @Nullable DimFilter filter
  )
  {
    this.dataSource = dataSource;
    this.intervals = intervals == null ? Intervals.ONLY_ETERNITY : intervals;
    this.filter = filter;
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
  public DimFilter getFilter()
  {
    return filter;
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
           && Objects.equals(filter, that.filter);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dataSource, intervals, filter);
  }

  @Override
  public String toString()
  {
    return "TableInputSpec{" +
           "dataSource='" + dataSource + '\'' +
           ", intervals=" + intervals +
           ", filter=" + filter +
           '}';
  }
}
