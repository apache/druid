/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.loading;

import io.druid.java.util.common.MapUtils;
import io.druid.segment.AbstractSegment;
import io.druid.segment.QueryableIndex;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.timeline.DataSegment;

import org.joda.time.Interval;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
*/
public class CacheTestSegmentLoader implements SegmentLoader
{

  private final Set<DataSegment> segmentsInTrash = new HashSet<>();

  @Override
  public boolean isSegmentLoaded(DataSegment segment) throws SegmentLoadingException
  {
    Map<String, Object> loadSpec = segment.getLoadSpec();
    return new File(MapUtils.getString(loadSpec, "cacheDir")).exists();
  }

  @Override
  public Segment getSegment(final DataSegment segment) throws SegmentLoadingException
  {
    return new AbstractSegment()
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
  public void cleanup(DataSegment segment) throws SegmentLoadingException
  {
    segmentsInTrash.add(segment);
  }

  public Set<DataSegment> getSegmentsInTrash()
  {
    return segmentsInTrash;
  }
}
