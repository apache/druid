/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.segment;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedInts;
import org.joda.time.Interval;

/**
 */
public class RowboatFilteringIndexAdapter implements IndexableAdapter
{
  private final IndexableAdapter baseAdapter;
  private final Predicate<Rowboat> filter;

  public RowboatFilteringIndexAdapter(IndexableAdapter baseAdapter, Predicate<Rowboat> filter)
  {
    this.baseAdapter = baseAdapter;
    this.filter = filter;
  }

  @Override
  public Interval getDataInterval()
  {
    return baseAdapter.getDataInterval();
  }

  @Override
  public int getNumRows()
  {
    return baseAdapter.getNumRows();
  }

  @Override
  public Indexed<String> getAvailableDimensions()
  {
    return baseAdapter.getAvailableDimensions();
  }

  @Override
  public Indexed<String> getAvailableMetrics()
  {
    return baseAdapter.getAvailableMetrics();
  }

  @Override
  public Indexed<String> getDimValueLookup(String dimension)
  {
    return baseAdapter.getDimValueLookup(dimension);
  }

  @Override
  public Iterable<Rowboat> getRows()
  {
    return Iterables.filter(baseAdapter.getRows(), filter);
  }

  @Override
  public IndexedInts getInverteds(String dimension, String value)
  {
    return baseAdapter.getInverteds(dimension, value);
  }

  @Override
  public String getMetricType(String metric)
  {
    return baseAdapter.getMetricType(metric);
  }
}
