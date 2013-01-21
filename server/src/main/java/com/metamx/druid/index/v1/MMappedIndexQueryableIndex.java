/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid.index.v1;

import com.metamx.druid.index.QueryableIndex;
import com.metamx.druid.index.column.Column;
import com.metamx.druid.index.column.ComplexColumnImpl;
import com.metamx.druid.index.column.FloatColumn;
import com.metamx.druid.index.column.LongColumn;
import com.metamx.druid.index.column.StringMultiValueColumn;
import com.metamx.druid.kv.Indexed;
import com.metamx.druid.kv.VSizeIndexed;
import org.joda.time.Interval;

/**
 */
public class MMappedIndexQueryableIndex implements QueryableIndex
{
  private final MMappedIndex index;

  public MMappedIndexQueryableIndex(
      MMappedIndex index
  )
  {
    this.index = index;
  }

  public MMappedIndex getIndex()
  {
    return index;
  }

  @Override
  public Interval getDataInterval()
  {
    return index.getDataInterval();
  }

  @Override
  public int getNumRows()
  {
    return index.getTimestamps().size();
  }

  @Override
  public Indexed<String> getColumnNames()
  {
    return null;
  }

  @Override
  public Indexed<String> getAvailableDimensions()
  {
    return index.getAvailableDimensions();
  }

  @Override
  public Column getTimeColumn()
  {
    return new LongColumn(index.timestamps);
  }

  @Override
  public Column getColumn(String columnName)
  {
    final MetricHolder metricHolder = index.getMetricHolder(columnName);
    if (metricHolder == null) {
      final VSizeIndexed dimColumn = index.getDimColumn(columnName);
      if (dimColumn == null) {
        return null;
      }

      return new StringMultiValueColumn(
          index.getDimValueLookup(columnName),
          dimColumn,
          index.getInvertedIndexes().get(columnName)
      );
    }
    else if (metricHolder.getType() == MetricHolder.MetricType.FLOAT) {
      return new FloatColumn(metricHolder.floatType);
    }
    else {
      return new ComplexColumnImpl(metricHolder.getTypeName(), metricHolder.getComplexType());
    }
  }
}
