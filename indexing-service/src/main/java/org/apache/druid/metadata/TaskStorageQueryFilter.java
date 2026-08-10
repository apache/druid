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

package org.apache.druid.metadata;

import org.apache.druid.indexer.TaskState;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.LikeDimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.RangeFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.filter.TypedInFilter;
import org.apache.druid.server.system.table.SystemTablePushdownFilter;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Immutable wrapper for the validated native filters pushed into task metadata queries. */
public final class TaskStorageQueryFilter
{
  private final List<DimFilter> filters;
  private final boolean includesActiveTasks;
  private final boolean includesCompleteTasks;

  public TaskStorageQueryFilter(final List<DimFilter> filters)
  {
    this.filters = List.copyOf(filters);
    final Set<Boolean> possibleTaskActiveness = computePossibleTaskActiveness();
    this.includesActiveTasks = possibleTaskActiveness.contains(true);
    this.includesCompleteTasks = possibleTaskActiveness.contains(false);
  }

  public List<DimFilter> getFilters()
  {
    return filters;
  }

  public String getStringValuesColumn(final DimFilter filter)
  {
    return SystemTablePushdownFilter.getStringValuesColumn(filter);
  }

  public Set<String> getStringValues(final DimFilter filter)
  {
    return SystemTablePushdownFilter.getStringValues(filter);
  }

  TaskStorageQueryFilter withoutStatusFilters()
  {
    return new TaskStorageQueryFilter(
        filters.stream().filter(filter -> !"status".equals(getColumn(filter))).toList()
    );
  }

  public boolean includesActiveTasks()
  {
    return includesActiveTasks;
  }

  public boolean includesCompleteTasks()
  {
    return includesCompleteTasks;
  }

  private Set<Boolean> getPossibleTaskActiveness(final DimFilter statusFilter)
  {
    final Set<Boolean> possibleActiveness = new HashSet<>();
    for (final String status : getStringValues(statusFilter)) {
      if (TaskState.RUNNING.name().equals(status)) {
        possibleActiveness.add(true);
      } else if (TaskState.SUCCESS.name().equals(status) || TaskState.FAILED.name().equals(status)) {
        possibleActiveness.add(false);
      }
    }
    return possibleActiveness;
  }

  private boolean isStringValuesFilter(final DimFilter filter)
  {
    return filter instanceof SelectorDimFilter
           || filter instanceof EqualityFilter
           || filter instanceof InDimFilter
           || filter instanceof TypedInFilter
           || filter instanceof OrDimFilter;
  }

  private Set<Boolean> computePossibleTaskActiveness()
  {
    final Set<Boolean> possibleActiveness = new HashSet<>(Set.of(true, false));
    for (final DimFilter filter : filters) {
      if (isStringValuesFilter(filter) && "status".equals(getStringValuesColumn(filter))) {
        possibleActiveness.retainAll(getPossibleTaskActiveness(filter));
      }
    }
    return possibleActiveness;
  }

  private String getColumn(final DimFilter filter)
  {
    return switch (filter) {
      case BoundDimFilter bound -> bound.getDimension();
      case RangeFilter range -> range.getColumn();
      case LikeDimFilter like -> like.getDimension();
      case NotDimFilter not -> getColumn(not.getField());
      default -> getStringValuesColumn(filter);
    };
  }
}
