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

package org.apache.druid.segment.join.filter;

import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.VirtualColumn;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Holds the result of splitting a filter into:
 * - a portion that can be pushed down to the base table
 * - a portion that will be applied post-join
 * - additional virtual columns that need to be created on the base table to support the pushed down filters.
 */
public class JoinFilterSplit
{
  final Optional<Filter> baseTableFilter;
  final Optional<Filter> joinTableFilter;
  final Set<VirtualColumn> pushDownVirtualColumns;

  public JoinFilterSplit(
      @Nullable Filter baseTableFilter,
      @Nullable Filter joinTableFilter,
      Set<VirtualColumn> pushDownVirtualColumns
  )
  {
    this.baseTableFilter = baseTableFilter == null ? Optional.empty() : Optional.of(baseTableFilter);
    this.joinTableFilter = joinTableFilter == null ? Optional.empty() : Optional.of(joinTableFilter);
    this.pushDownVirtualColumns = pushDownVirtualColumns;
  }

  public Optional<Filter> getBaseTableFilter()
  {
    return baseTableFilter;
  }

  public Optional<Filter> getJoinTableFilter()
  {
    return joinTableFilter;
  }

  public Set<VirtualColumn> getPushDownVirtualColumns()
  {
    return pushDownVirtualColumns;
  }

  @Override
  public String toString()
  {
    return "JoinFilterSplit{" +
           "baseTableFilter=" + baseTableFilter +
           ", joinTableFilter=" + joinTableFilter +
           ", pushDownVirtualColumns=" + pushDownVirtualColumns +
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
    JoinFilterSplit that = (JoinFilterSplit) o;
    return Objects.equals(getBaseTableFilter(), that.getBaseTableFilter()) &&
           Objects.equals(getJoinTableFilter(), that.getJoinTableFilter()) &&
           Objects.equals(getPushDownVirtualColumns(), that.getPushDownVirtualColumns());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(getBaseTableFilter(), getJoinTableFilter(), getPushDownVirtualColumns());
  }
}
