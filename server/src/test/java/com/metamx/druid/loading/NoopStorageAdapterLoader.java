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

package com.metamx.druid.loading;

import com.metamx.druid.Capabilities;
import com.metamx.druid.QueryGranularity;
import com.metamx.druid.StorageAdapter;
import com.metamx.druid.index.brita.Filter;
import com.metamx.druid.index.v1.processing.Cursor;
import com.metamx.druid.query.search.SearchHit;
import com.metamx.druid.query.search.SearchQuery;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.Map;

/**
*/
public class NoopStorageAdapterLoader implements StorageAdapterLoader
{
  @Override
  public StorageAdapter getAdapter(final Map<String, Object> loadSpec)
  {
    return new StorageAdapter()
    {
      @Override
      public String getSegmentIdentifier()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public Interval getInterval()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public int getDimensionCardinality(String dimension)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public DateTime getMinTime()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public DateTime getMaxTime()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public Capabilities getCapabilities()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public Iterable<Cursor> makeCursors(Filter filter, Interval interval, QueryGranularity gran)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public Iterable<SearchHit> searchDimensions(SearchQuery query, Filter filter)
      {
        throw new UnsupportedOperationException();
      }
    };
  }

  @Override
  public void cleanupAdapter(Map<String, Object> loadSpec) throws StorageAdapterLoadingException
  {

  }
}
