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

import org.apache.druid.segment.ReferenceCountedObjectProvider;
import org.apache.druid.segment.Segment;

import java.util.Optional;

/**
 * Wraps a {@link ReferenceCountedObjectProvider<Segment>} with additional measurements about segment loading, if it
 * was required
 */
public class AcquireSegmentResult
{
  private static final AcquireSegmentResult EMPTY = new AcquireSegmentResult(Optional::empty, 0L, 0L, 0L);

  public static AcquireSegmentResult empty()
  {
    return EMPTY;
  }

  public static AcquireSegmentResult cached(ReferenceCountedObjectProvider<Segment> provider)
  {
    return new AcquireSegmentResult(provider, 0L, 0L, 0L);
  }

  private final ReferenceCountedObjectProvider<Segment> referenceProvider;
  private final long loadSizeBytes;
  private final long waitTimeNanos;
  private final long loadTimeNanos;

  public AcquireSegmentResult(
      ReferenceCountedObjectProvider<Segment> referenceProvider,
      long loadSizeBytes,
      long waitTimeNanos,
      long loadTimeNanos
  )
  {
    this.referenceProvider = referenceProvider;
    this.loadSizeBytes = loadSizeBytes;
    this.waitTimeNanos = waitTimeNanos;
    this.loadTimeNanos = loadTimeNanos;
  }

  /**
   * Segment reference provider for loaded segment
   */
  public ReferenceCountedObjectProvider<Segment> getReferenceProvider()
  {
    return referenceProvider;
  }

  /**
   * Amount of data loaded into the cache, or 0 if it was already available in the cache
   */
  public long getLoadSizeBytes()
  {
    return loadSizeBytes;
  }

  /**
   * Amount of time spent waiting before actually loading the segment (e.g. if loads are done on a shared thread pool)
   */
  public long getWaitTimeNanos()
  {
    return waitTimeNanos;
  }

  /**
   * Amount of time spent loading a segment, or 0 if the segment was already available in the cache
   */
  public long getLoadTimeNanos()
  {
    return loadTimeNanos;
  }
}
