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

import org.apache.druid.guice.annotations.UnstableApi;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.SegmentLazyLoadFailCallback;
import org.apache.druid.timeline.DataSegment;

import java.util.concurrent.ExecutorService;

/**
 * Loading segments from deep storage to local storage. Internally, this class can delegate the download to
 * {@link SegmentCacheManager}. Implementations must be thread-safe.
 */
@UnstableApi
public interface SegmentLoader
{

  /**
   * Returns a {@link ReferenceCountingSegment} that will be added by the {@link org.apache.druid.server.SegmentManager}
   * to the {@link org.apache.druid.timeline.VersionedIntervalTimeline}. This method can be called multiple times
   * by the {@link org.apache.druid.server.SegmentManager} and implementation can either return same {@link ReferenceCountingSegment}
   * or a different {@link ReferenceCountingSegment}. Caller should not assume any particular behavior.
   *
   * Returning a {@code ReferenceCountingSegment} will let custom implementations keep track of reference count for
   * segments that the custom implementations are creating. That way, custom implementations can know when the segment
   * is in use or not.
   * @param segment - Segment to load
   * @param lazy - Whether column metadata de-serialization is to be deferred to access time. Setting this flag to true can speed up segment loading
   * @param loadFailed - Callback to invoke if lazy loading fails during column access.
   * @throws SegmentLoadingException - If there is an error in loading the segment
   */
  ReferenceCountingSegment getSegment(DataSegment segment, boolean lazy, SegmentLazyLoadFailCallback loadFailed) throws SegmentLoadingException;

  /**
   * cleanup any state used by this segment
   */
  void cleanup(DataSegment segment);

  /**
   * Asyncly load segment into page cache.
   * Equivalent to `cat segment_files > /dev/null` to force loading the segment index files into page cache so that
   * later when the segment is queried, they are already in page cache and only a minor page fault needs to be triggered
   * instead of a major page fault to make the query latency more consistent.
   *
   * @param segment The segment to load its index files into page cache
   * @param exec The thread pool to use
   */
  void loadSegmentIntoPageCache(DataSegment segment, ExecutorService exec);
}
