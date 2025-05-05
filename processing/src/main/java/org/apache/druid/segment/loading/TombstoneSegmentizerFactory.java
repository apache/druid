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
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.NoopQueryableIndex;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentLazyLoadFailCallback;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;

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
    final QueryableIndex queryableIndex = new NoopQueryableIndex()
    {
      @Override
      public Interval getDataInterval()
      {
        return tombstone.getInterval();
      }
    };

    final Segment segmentObject = new Segment()
    {
      @Override
      public SegmentId getId()
      {
        return tombstone.getId();
      }

      @Override
      public Interval getDataInterval()
      {
        return tombstone.getInterval();
      }

      @Nullable
      @Override
      public <T> T as(@Nonnull Class<T> clazz)
      {
        if (CursorFactory.class.equals(clazz)) {
          return (T) new CursorFactory()
          {
            @Override
            public CursorHolder makeCursorHolder(CursorBuildSpec spec)
            {
              return new CursorHolder()
              {
                @Nullable
                @Override
                public Cursor asCursor()
                {
                  return null;
                }
              };
            }

            @Override
            public RowSignature getRowSignature()
            {
              return RowSignature.empty();
            }

            @Override
            @Nullable
            public ColumnCapabilities getColumnCapabilities(String column)
            {
              return null;
            }
          };
        } else if (QueryableIndex.class.equals(clazz)) {
          return (T) queryableIndex;
        }
        return null;
      }

      @Override
      public boolean isTombstone()
      {
        return true;
      }

      @Override
      public void close()
      {

      }
    };
    return segmentObject;
  }
}

