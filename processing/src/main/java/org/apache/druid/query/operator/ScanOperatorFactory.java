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

package org.apache.druid.query.operator;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.segment.VirtualColumns;
import org.joda.time.Interval;

import java.util.List;
import java.util.Objects;

public class ScanOperatorFactory implements OperatorFactory
{
  private final Interval timeRange;
  private final DimFilter filter;
  private final OffsetLimit offsetLimit;
  private final List<String> projectedColumns;
  private final VirtualColumns virtualColumns;
  private final List<ColumnWithDirection> ordering;

  public ScanOperatorFactory(
      @JsonProperty("timeRange") final Interval timeRange,
      @JsonProperty("filter") final DimFilter filter,
      @JsonProperty("offsetLimit") final OffsetLimit offsetLimit,
      @JsonProperty("projectedColumns") final List<String> projectedColumns,
      @JsonProperty("virtualColumns") final VirtualColumns virtualColumns,
      @JsonProperty("ordering") final List<ColumnWithDirection> ordering
  )
  {
    this.timeRange = timeRange;
    this.filter = filter;
    this.offsetLimit = offsetLimit;
    this.projectedColumns = projectedColumns;
    this.virtualColumns = virtualColumns;
    this.ordering = ordering;
  }

  @JsonProperty
  public Interval getTimeRange()
  {
    return timeRange;
  }

  @JsonProperty
  public DimFilter getFilter()
  {
    return filter;
  }

  @JsonProperty
  public OffsetLimit getOffsetLimit()
  {
    return offsetLimit;
  }

  @JsonProperty
  public List<String> getProjectedColumns()
  {
    return projectedColumns;
  }

  @JsonProperty
  public VirtualColumns getVirtualColumns()
  {
    return virtualColumns;
  }

  @JsonProperty
  public List<ColumnWithDirection> getOrdering()
  {
    return ordering;
  }

  @Override
  public Operator wrap(Operator op)
  {
    return new ScanOperator(
        op,
        projectedColumns,
        virtualColumns,
        timeRange,
        filter == null ? null : filter.toFilter(),
        ordering,
        offsetLimit
    );
  }

  @Override
  public boolean validateEquivalent(OperatorFactory other)
  {
    return equals(other);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ScanOperatorFactory)) {
      return false;
    }
    ScanOperatorFactory that = (ScanOperatorFactory) o;
    return Objects.equals(offsetLimit, that.offsetLimit)
        && Objects.equals(timeRange, that.timeRange)
        && Objects.equals(filter, that.filter)
        && Objects.equals(projectedColumns, that.projectedColumns)
        && Objects.equals(virtualColumns, that.virtualColumns)
        && Objects.equals(ordering, that.ordering);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(timeRange, filter, offsetLimit, projectedColumns, virtualColumns, ordering);
  }

  @Override
  public String toString()
  {
    return "ScanOperatorFactory{" +
        "timeRange=" + timeRange +
        ", filter=" + filter +
        ", offsetLimit=" + offsetLimit +
        ", projectedColumns=" + projectedColumns +
        ", virtualColumns=" + virtualColumns +
        ", ordering=" + ordering
        + "}";
  }


}
