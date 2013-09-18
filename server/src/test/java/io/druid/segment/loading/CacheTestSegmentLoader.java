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

package io.druid.segment.loading;

import com.metamx.common.MapUtils;
import io.druid.segment.QueryableIndex;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.timeline.DataSegment;
import org.joda.time.Interval;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
*/
public class CacheTestSegmentLoader implements SegmentLoader
{
  @Override
  public boolean isSegmentLoaded(DataSegment segment) throws SegmentLoadingException
  {
    Map<String, Object> loadSpec = segment.getLoadSpec();
    return new File(MapUtils.getString(loadSpec, "cacheDir")).exists();
  }

  @Override
  public Segment getSegment(final DataSegment segment) throws SegmentLoadingException
  {
    return new Segment()
    {
      @Override
      public String getIdentifier()
      {
        return segment.getIdentifier();
      }

      @Override
      public Interval getDataInterval()
      {
        return segment.getInterval();
      }

      @Override
      public QueryableIndex asQueryableIndex()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public StorageAdapter asStorageAdapter()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public void close() throws IOException
      {
      }
    };
  }

  @Override
  public File getSegmentFiles(DataSegment segment) throws SegmentLoadingException
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void cleanup(DataSegment loadSpec) throws SegmentLoadingException
  {
  }
}
