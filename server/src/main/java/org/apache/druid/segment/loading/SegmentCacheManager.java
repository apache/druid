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

import org.apache.druid.timeline.DataSegment;

import java.io.File;
import java.util.concurrent.ExecutorService;

/**
 * A class to fetch segment files to local disk and manage the local cache.
 * Implementations must be thread-safe.
 */
public interface SegmentCacheManager
{
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

  /**
   * Tries to reserve the space for a segment on any location. When the space has been reserved,
   * {@link #getSegmentFiles(DataSegment)} should download the segment on the reserved location or
   * fail otherwise.
   *
   * This function is useful for custom extensions. Extensions can try to reserve the space first and
   * if not successful, make some space by cleaning up other segments, etc. There is also improved
   * concurrency for extensions with this function. Since reserve is a cheaper operation to invoke
   * till the space has been reserved. Hence it can be put inside a lock if required by the extensions. getSegment
   * can't be put inside a lock since it is a time-consuming operation, on account of downloading the files.
   *
   * @param segment - Segment to reserve
   * @return True if enough space found to store the segment, false otherwise
   */
  /*
   * We only return a boolean result instead of a pointer to
   * {@link StorageLocation} since we don't want callers to operate on {@code StorageLocation} directly outside {@code SegmentLoader}.
   * {@link SegmentLoader} operates on the {@code StorageLocation} objects in a thread-safe manner.
   */
  boolean reserve(DataSegment segment);

  /**
   * Reverts the effects of {@link #reserve(DataSegment)} (DataSegment)} by releasing the location reserved for this segment.
   * Callers, that explicitly reserve the space via {@link #reserve(DataSegment)}, should use this method to release the space.
   *
   * Implementation can throw error if the space is being released but there is data present. Callers
   * are supposed to ensure that any data is removed via {@link #cleanup(DataSegment)}
   * @param segment - Segment to release the location for.
   * @return - True if any location was reserved and released, false otherwise.
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
   * @param exec The thread pool to use
   */
  void loadSegmentIntoPageCache(DataSegment segment, ExecutorService exec);
}
