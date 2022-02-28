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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.segment.DimensionHandler;
import org.apache.druid.segment.Metadata;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentLazyLoadFailCallback;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.util.List;
import java.util.Map;

public class TombstoneSegmentizerFactory implements SegmentizerFactory
{
  @Override
  public Segment factorize(
      DataSegment segment,
      File parentDir,
      boolean lazy,
      SegmentLazyLoadFailCallback loadFailed
  )
  {
    return segmentForTombstone(segment);
  }

  @VisibleForTesting
  public static Segment segmentForTombstone(DataSegment tombstone)
  {
    Preconditions.checkArgument(tombstone.isTombstone());

    // Create a no-op queryable index that indicates that it was created from a tombstone...then the
    // server manager will use the information to short-circuit and create a no-op query runner for
    // it since it has no data:
    final QueryableIndex queryableIndex =
        new QueryableIndex()
        {
          @Override
          public Interval getDataInterval()
          {
            return tombstone.getInterval();
          }

          @Override
          public int getNumRows()
          {
            throw new UnsupportedOperationException();
          }

          @Override
          public Indexed<String> getAvailableDimensions()
          {
            throw new UnsupportedOperationException();
          }

          @Override
          public BitmapFactory getBitmapFactoryForDimensions()
          {
            throw new UnsupportedOperationException();
          }

          @Nullable
          @Override
          public Metadata getMetadata()
          {
            throw new UnsupportedOperationException();
          }

          @Override
          public Map<String, DimensionHandler> getDimensionHandlers()
          {
            throw new UnsupportedOperationException();
          }

          @Override
          public void close()
          {

          }

          @Override
          public List<String> getColumnNames()
          {
            throw new UnsupportedOperationException();
          }

          @Nullable
          @Override
          public ColumnHolder getColumnHolder(String columnName)
          {
            throw new UnsupportedOperationException();
          }

          // mark this index to indicate that it comes from a tombstone:
          @Override
          public boolean isFromTombstone()
          {
            return true;
          }
        };

    final QueryableIndexStorageAdapter storageAdapter = new QueryableIndexStorageAdapter(queryableIndex);

    Segment segmentObject = new Segment()
    {
      @Override
      public SegmentId getId()
      {
        return tombstone.getId();
      }

      @Override
      public Interval getDataInterval()
      {
        return asQueryableIndex().getDataInterval();
      }

      @Nullable
      @Override
      public QueryableIndex asQueryableIndex()
      {
        return queryableIndex;
      }

      @Override
      public StorageAdapter asStorageAdapter()
      {
        return storageAdapter;
      }

      @Override
      public void close()
      {

      }
    };
    return segmentObject;
  }
}

