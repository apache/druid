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

import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.SegmentLazyLoadFailCallback;
import org.apache.druid.timeline.DataSegment;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * A class to fetch segment files to local disk and manage the local cache.
 * Implementations must be thread-safe.
 */
public interface SegmentCacheManager
{
  /**
   * Return whether the cache manager can handle segments or not.
   */
  boolean canHandleSegments();

  /**
   * Return a list of cached segments from local disk, if any. This should be called only
   * when {@link #canHandleSegments()} is true.
   */
  List<DataSegment> getCachedSegments() throws IOException;

  /**
   * Store a segment info file for the supplied segment on disk. This operation is idempotent when called
   * multiple times for a given segment.
   */
  void storeInfoFile(DataSegment segment) throws IOException;

  /**
   * Remove the segment info file for the supplied segment from disk. If the file cannot be
   * deleted, do nothing.
   *
   * @see SegmentCacheManager#cleanup(DataSegment)
   */
  void removeInfoFile(DataSegment segment);

  /**
   * Returns a {@link ReferenceCountingSegment} that will be added by the {@link org.apache.druid.server.SegmentManager}
   * to the {@link org.apache.druid.timeline.VersionedIntervalTimeline}. This method can be called multiple times
   * by the {@link org.apache.druid.server.SegmentManager} and implementation can either return same {@link ReferenceCountingSegment}
   * or a different {@link ReferenceCountingSegment}. Caller should not assume any particular behavior.
   * <p>
   * Returning a {@code ReferenceCountingSegment} will let custom implementations keep track of reference count for
   * segments that the custom implementations are creating. That way, custom implementations can know when the segment
   * is in use or not.
   * </p>
   * @param segment Segment to get on each download after service bootstrap
   * @throws SegmentLoadingException If there is an error in loading the segment
   * @see SegmentCacheManager#getBootstrapSegment(DataSegment, SegmentLazyLoadFailCallback)
   */
  ReferenceCountingSegment getSegment(DataSegment segment) throws SegmentLoadingException;

  /**
   * Similar to {@link #getSegment(DataSegment)}, this method returns a {@link ReferenceCountingSegment} that will be
   * added by the {@link org.apache.druid.server.SegmentManager} to the {@link org.apache.druid.timeline.VersionedIntervalTimeline}
   * during startup on data nodes.
   * @param segment Segment to retrieve during service bootstrap
   * @param loadFailed Callback to execute when segment lazy load failed. This applies only when
   *                   {@code lazyLoadOnStart} is enabled
   * @throws SegmentLoadingException - If there is an error in loading the segment
   * @see SegmentCacheManager#getSegment(DataSegment)
   */
  ReferenceCountingSegment getBootstrapSegment(
      DataSegment segment,
      SegmentLazyLoadFailCallback loadFailed
  ) throws SegmentLoadingException;

  /**
   * This method fetches the files for the given segment if the segment is not downloaded already. It
   * is not required to {@link #reserve(DataSegment)} before calling this method. If caller has not reserved
   * the space explicitly via {@link #reserve(DataSegment)}, the implementation should reserve space on caller's
   * behalf.
   * If the space has been explicitly reserved already
   *    - implementation should use only the reserved space to store segment files.
   *    - implementation should not release the location in case of download erros and leave it to the caller.
   * @throws SegmentLoadingException if there is an error in downloading files
   */
  File getSegmentFiles(DataSegment segment) throws SegmentLoadingException;

  /**
   * Asynchronously load the supplied segment into the page cache on each download after the service finishes bootstrapping.
   * Equivalent to `cat segment_files > /dev/null` to force loading the segment index files into page cache so that
   * later when the segment is queried, they are already in page cache and only a minor page fault needs to be triggered
   * instead of a major page fault to make the query latency more consistent.
   *
   * @see SegmentCacheManager#loadSegmentIntoPageCacheOnBootstrap(DataSegment)
   */
  void loadSegmentIntoPageCache(DataSegment segment);

  /**
   * Similar to {@link #loadSegmentIntoPageCache(DataSegment)}, but asynchronously load the supplied segment into the
   * page cache during service bootstrap.
   *
   * @see SegmentCacheManager#loadSegmentIntoPageCache(DataSegment)
   */
  void loadSegmentIntoPageCacheOnBootstrap(DataSegment segment);

  /**
   * Shutdown any previously set up bootstrap executor to save resources.
   * This should be called after loading bootstrap segments into the page cache.
   */
  void shutdownBootstrap();

  boolean reserve(DataSegment segment);

  /**
   * Reverts the effects of {@link #reserve(DataSegment)} by releasing the location reserved for this segment.
   * Callers that explicitly reserve the space via {@link #reserve(DataSegment)} should use this method to release the space.
   *
   * <p>
   * Implementation can throw error if the space is being released but there is data present. Callers
   * are supposed to ensure that any data is removed via {@link #cleanup(DataSegment)}. Only return a boolean instead
   * of a pointer to {@code StorageLocation} since we don't want callers to operate on {@code StorageLocation} directly
   * outside this interface.
   * </p>
   *
   * @param segment - Segment to release the location for.
   * @return - True if any location was reserved and released, false otherwise.
   *
   */
  boolean release(DataSegment segment);

  /**
   * Cleanup the segment files cache space used by the segment. It will not release the space if the
   * space has been explicitly reserved via {@link #reserve(DataSegment)}.
   *
   * @see SegmentCacheManager#removeInfoFile(DataSegment)
   */
  void cleanup(DataSegment segment);
}
