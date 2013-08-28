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

import com.metamx.druid.query.search.SearchHit;
import com.metamx.druid.query.search.SearchQuery;
import io.druid.granularity.QueryGranularity;
import io.druid.query.filter.Filter;
import io.druid.segment.Capabilities;
import io.druid.segment.Cursor;
import io.druid.segment.StorageAdapter;
import org.joda.time.DateTime;
import org.joda.time.Interval;

/**
 */
public class SegmentIdAttachedStorageAdapter implements StorageAdapter
{
  private final String segmentId;
  private final StorageAdapter delegate;

  public SegmentIdAttachedStorageAdapter(
      String segmentId,
      StorageAdapter delegate
  )
  {
    this.segmentId = segmentId;
    this.delegate = delegate;
  }

  @Override
  public String getSegmentIdentifier()
  {
    return segmentId;
  }

  @Override
  public Interval getInterval()
  {
    return delegate.getInterval();
  }

  @Override
  public Iterable<SearchHit> searchDimensions(SearchQuery query, Filter filter)
  {
    return delegate.searchDimensions(query, filter);
  }

  @Override
  public Iterable<Cursor> makeCursors(Filter filter, Interval interval, QueryGranularity gran)
  {
    return delegate.makeCursors(filter, interval, gran);
  }

  @Override
  public Capabilities getCapabilities()
  {
    return delegate.getCapabilities();
  }

  @Override
  public DateTime getMaxTime()
  {
    return delegate.getMaxTime();
  }

  @Override
  public DateTime getMinTime()
  {
    return delegate.getMinTime();
  }

  @Override
  public int getDimensionCardinality(String dimension)
  {
    return delegate.getDimensionCardinality(dimension);
  }

  public StorageAdapter getDelegate()
  {
    return delegate;
  }
}
