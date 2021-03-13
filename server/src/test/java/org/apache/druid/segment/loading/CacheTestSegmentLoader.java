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

import org.apache.druid.java.util.common.MapUtils;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentLazyLoadFailCallback;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import java.io.File;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
*/
public class CacheTestSegmentLoader implements SegmentLoader
{
  private final Set<DataSegment> segmentsInTrash = new HashSet<>();

  @Override
  public boolean isSegmentLoaded(DataSegment segment)
  {
    Map<String, Object> loadSpec = segment.getLoadSpec();
    return new File(MapUtils.getString(loadSpec, "cacheDir")).exists();
  }

  @Override
  public Segment getSegment(final DataSegment segment, boolean lazy, SegmentLazyLoadFailCallback SegmentLazyLoadFailCallback)
  {
    return new Segment()
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
        throw new UnsupportedOperationException();
      }

      @Override
      public void close()
      {
      }
    };
  }

  @Override
  public File getSegmentFiles(DataSegment segment)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void cleanup(DataSegment segment)
  {
    segmentsInTrash.add(segment);
  }

  public Set<DataSegment> getSegmentsInTrash()
  {
    return segmentsInTrash;
  }
}
