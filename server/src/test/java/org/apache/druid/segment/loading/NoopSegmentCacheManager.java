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

import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentLazyLoadFailCallback;
import org.apache.druid.timeline.DataSegment;

import java.io.File;
import java.util.Collection;
import java.util.Optional;

/**
 * Test implementation of {@link SegmentCacheManager} which throws an
 * {@link UnsupportedOperationException} on invocation of any method.
 */
public class NoopSegmentCacheManager implements SegmentCacheManager
{
  @Override
  public boolean canHandleSegments()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Collection<DataSegment> getCachedSegments()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void storeInfoFile(DataSegment segment)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void removeInfoFile(DataSegment segment)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void load(DataSegment segment)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void bootstrap(DataSegment segment, SegmentLazyLoadFailCallback loadFailed)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public File getSegmentFiles(DataSegment segment)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void drop(DataSegment segment)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Optional<Segment> acquireCachedSegment(DataSegment dataSegment)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public AcquireSegmentAction acquireSegment(DataSegment dataSegment)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void shutdownBootstrap()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void shutdown()
  {
    throw new UnsupportedOperationException();
  }
}
