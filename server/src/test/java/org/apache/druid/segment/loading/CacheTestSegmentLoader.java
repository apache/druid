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

package org.apache.druid.segment.loading;

import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.Metadata;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentLazyLoadFailCallback;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.concurrent.ExecutorService;

/**
*/
public class CacheTestSegmentLoader implements SegmentLoader
{

  @Override
  public ReferenceCountingSegment getSegment(final DataSegment segment, boolean lazy, SegmentLazyLoadFailCallback SegmentLazyLoadFailCallback)
  {
    Segment baseSegment = new Segment()
    {
      @Override
      public SegmentId getId()
      {
        return segment.getId();
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
        return new StorageAdapter()
        {
          @Override
          public Interval getInterval()
          {
            throw new UnsupportedOperationException();
          }

          @Override
          public Indexed<String> getAvailableDimensions()
          {
            throw new UnsupportedOperationException();
          }

          @Override
          public Iterable<String> getAvailableMetrics()
          {
            throw new UnsupportedOperationException();
          }

          @Override
          public int getDimensionCardinality(String column)
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

          @Nullable
          @Override
          public Comparable getMinValue(String column)
          {
            throw new UnsupportedOperationException();
          }

          @Nullable
          @Override
          public Comparable getMaxValue(String column)
          {
            throw new UnsupportedOperationException();
          }

          @Nullable
          @Override
          public ColumnCapabilities getColumnCapabilities(String column)
          {
            throw new UnsupportedOperationException();
          }

          @Override
          public int getNumRows()
          {
            return 1;
          }

          @Override
          public DateTime getMaxIngestedEventTime()
          {
            throw new UnsupportedOperationException();
          }

          @Override
          public Metadata getMetadata()
          {
            throw new UnsupportedOperationException();
          }

          @Override
          public Sequence<Cursor> makeCursors(
              @Nullable Filter filter,
              Interval interval,
              VirtualColumns virtualColumns,
              Granularity gran,
              boolean descending,
              @Nullable QueryMetrics<?> queryMetrics
          )
          {
            throw new UnsupportedOperationException();
          }
        };
      }

      @Override
      public void close()
      {
      }
    };
    return ReferenceCountingSegment.wrapSegment(baseSegment, segment.getShardSpec());
  }

  @Override
  public void loadSegmentIntoPageCache(DataSegment segment, ExecutorService exec)
  {

  }

  @Override
  public void cleanup(DataSegment segment)
  {

  }
}
