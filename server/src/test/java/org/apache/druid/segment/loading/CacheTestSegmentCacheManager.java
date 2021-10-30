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
import org.apache.druid.timeline.DataSegment;

import java.io.File;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 *
 */
public class CacheTestSegmentCacheManager implements SegmentCacheManager
{
  private final Set<DataSegment> segmentsInTrash = new HashSet<>();

  @Override
  public boolean isSegmentCached(DataSegment segment)
  {
    Map<String, Object> loadSpec = segment.getLoadSpec();
    return new File(MapUtils.getString(loadSpec, "cacheDir")).exists();
  }

  @Override
  public File getSegmentFiles(DataSegment segment)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean reserve(DataSegment segment)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean release(DataSegment segment)
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

  @Override
  public void loadSegmentIntoPageCache(DataSegment segment, ExecutorService exec)
  {
    throw new UnsupportedOperationException();
  }
}
