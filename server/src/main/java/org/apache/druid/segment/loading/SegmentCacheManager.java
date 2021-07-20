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

/**
 * A class to fetch segment files to local disk and manage the local cache.
 * Implementations must be thread-safe.
 */
public interface SegmentCacheManager
{
  /**
   * Checks whether a segment is already cached.
   */
  boolean isSegmentCached(DataSegment segment);

  /**
   * This method fetches the files for the given segment if the segment is not downloaded already.
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
   * @param segment - Segment to release the location for.
   * @return - True if any location was reserved and released, false otherwise.
   */
  boolean release(DataSegment segment);

  /**
   * Cleanup the cache space used by the segment
   */
  void cleanup(DataSegment segment);
}
