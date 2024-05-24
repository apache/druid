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

import javax.annotation.Nullable;
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
   * Return the set of cached segments from local disk. This should be called only
   * when {@link #canHandleSegments()} is true.
   */
  List<DataSegment> getCachedSegments() throws IOException;

  /**
   * Store a segment info file the supplied segment on disk. This operation is idempotent when called
   * multiple times for a given segment.
   */
  void storeInfoFile(DataSegment segment) throws IOException;

  /**
   * Remove the segment info file for the supplied segment from disk. If the file cannot be
   * deleted, do nothing.
   */
  void removeInfoFile(DataSegment segment);

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
  @Nullable
  ReferenceCountingSegment getSegment(
      DataSegment segment
  ) throws SegmentLoadingException;


  /**
   *
   * @param segment
   * @param loadFailed
   * @return
   * @throws SegmentLoadingException
   */
  @Nullable
  ReferenceCountingSegment getBootstrapSegment(
      DataSegment segment,
      SegmentLazyLoadFailCallback loadFailed
  ) throws SegmentLoadingException;

  /**
   * Checks whether a segment is already cached. It can return false even if {@link #reserve(DataSegment)}
   * has been successful for a segment but is not downloaded yet.
   */
  boolean isSegmentCached(DataSegment segment);

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
   * Cleanup the cache space used by the segment. It will not release the space if the space has been
   * explicitly reserved via {@link #reserve(DataSegment)}
   */
  void cleanup(DataSegment segment);

  /**
   * Asyncly load segment into page cache.
   * Equivalent to `cat segment_files > /dev/null` to force loading the segment index files into page cache so that
   * later when the segment is queried, they are already in page cache and only a minor page fault needs to be triggered
   * instead of a major page fault to make the query latency more consistent.
   *
   * @param segment The segment to load its index files into page cache
   */
  void loadSegmentIntoPageCache(DataSegment segment);

  /**
   *
   */
  void loadSegmentIntoPageCacheOnBootstrap(DataSegment segment);
}
