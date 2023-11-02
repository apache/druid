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
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.segment.VirtualColumns;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;

public class TestRowsAndColumnsDecorator implements RowsAndColumnsDecorator
{
  private Interval timeRange;
  private Filter filter;
  private VirtualColumns virtualColumns;
  private OffsetLimit offsetLimit = OffsetLimit.NONE;
  private List<ColumnWithDirection> ordering;
  private List<String> projectedColumns;

  @Override
  public void limitTimeRange(Interval interval)
  {
    this.timeRange = interval;
  }

  @Override
  public void addFilter(Filter filter)
  {
    this.filter = filter;
  }

  @Override
  public void addVirtualColumns(VirtualColumns virtualColumns)
  {
    this.virtualColumns = virtualColumns;
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
    this.projectedColumns = columns;
    return toRowsAndColumns();
  }

  @Override
  public RowsAndColumns toRowsAndColumns()
  {
    return new DecoratedRowsAndColumns();
  }

  public class DecoratedRowsAndColumns implements RowsAndColumns
  {
    public Interval getTimeRange()
    {
      return timeRange;
    }

    public Filter getFilter()
    {
      return filter;
    }

    public VirtualColumns getVirtualColumns()
    {
      return virtualColumns;
    }

    public OffsetLimit getOffsetLimit()
    {
      return offsetLimit;
    }

    public List<ColumnWithDirection> getOrdering()
    {
      return ordering;
    }

    public List<String> getProjectedColumns()
    {
      return projectedColumns;
    }


    @Override
    public Collection<String> getColumnNames()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public int numRows()
    {
      throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Column findColumn(String name)
    {
      throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public <T> T as(Class<T> clazz)
    {
      throw new UnsupportedOperationException();
    }
  }
}
