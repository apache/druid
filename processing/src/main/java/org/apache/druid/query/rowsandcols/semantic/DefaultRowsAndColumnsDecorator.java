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

package org.apache.druid.query.rowsandcols.semantic;

import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.operator.ColumnWithDirection;
import org.apache.druid.query.operator.OffsetLimit;
import org.apache.druid.query.rowsandcols.LazilyDecoratedRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.filter.AndFilter;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;

public class DefaultRowsAndColumnsDecorator implements RowsAndColumnsDecorator
{
  private RowsAndColumns base;
  private Interval interval;
  private Filter filter;
  private VirtualColumns virtualColumns;
  private OffsetLimit offsetLimit;
  private List<ColumnWithDirection> ordering;

  public DefaultRowsAndColumnsDecorator(
      RowsAndColumns base
  )
  {
    this(base, null, null, null, OffsetLimit.NONE, null);
  }

  public DefaultRowsAndColumnsDecorator(
      RowsAndColumns base,
      Interval interval,
      Filter filter,
      VirtualColumns virtualColumns,
      OffsetLimit limit,
      List<ColumnWithDirection> ordering
  )
  {
    this.base = base;
    this.interval = interval;
    this.filter = filter;
    this.virtualColumns = virtualColumns;
    this.offsetLimit = limit;
    this.ordering = ordering;
  }

  @Override
  public void limitTimeRange(Interval interval)
  {
    if (this.interval == null) {
      this.interval = interval;
    } else {
      this.interval = this.interval.overlap(interval);
    }
  }

  @Override
  public void addFilter(Filter filter)
  {
    if (this.filter == null) {
      this.filter = filter;
    } else {
      LinkedHashSet<Filter> newFilters = new LinkedHashSet<>();
      if (this.filter instanceof AndFilter) {
        newFilters.addAll(((AndFilter) this.filter).getFilters());
      } else {
        newFilters.add(this.filter);
      }

      newFilters.add(filter);
      this.filter = new AndFilter(newFilters);
    }
  }

  @Override
  public void addVirtualColumns(VirtualColumns virtualColumns)
  {
    if (this.virtualColumns == null) {
      this.virtualColumns = virtualColumns;
    } else {
      final VirtualColumn[] existing = this.virtualColumns.getVirtualColumns();
      final VirtualColumn[] incoming = virtualColumns.getVirtualColumns();
      ArrayList<VirtualColumn> cols = new ArrayList<>(existing.length + incoming.length);
      cols.addAll(Arrays.asList(existing));
      cols.addAll(Arrays.asList(incoming));

      this.virtualColumns = VirtualColumns.create(cols);
    }
  }

  @Override
  public void setOffsetLimit(OffsetLimit offsetLimit)
  {
    this.offsetLimit = offsetLimit;
  }

  @Override
  public void setOrdering(List<ColumnWithDirection> ordering)
  {
    this.ordering = ordering;
  }

  @Override
  public RowsAndColumns restrictColumns(List<String> columns)
  {
    return new LazilyDecoratedRowsAndColumns(
        base,
        interval,
        filter,
        virtualColumns,
        offsetLimit,
        ordering,
        columns == null ? null : new LinkedHashSet<>(columns)
    );
  }

  @Override
  public RowsAndColumns toRowsAndColumns()
  {
    return restrictColumns(null);
  }

}
