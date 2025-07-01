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

import io.github.resilience4j.core.lang.Nullable;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.segment.ReferenceCountedSegmentProvider;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentLazyLoadFailCallback;
import org.apache.druid.segment.SegmentMapFunction;
import org.apache.druid.timeline.DataSegment;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * A class to fetch segment files from deep storage to local disk and manage the local cache. Implementations must be
 * thread-safe.
 */
public interface SegmentCacheManager
{
  /**
   * Return whether the cache manager can handle segments or not.
   */
  boolean canHandleSegments();

  /**
   * Return a list of cached segments from local disk, if any. This should be called only when
   * {@link #canHandleSegments()} is true.
   */
  List<DataSegment> getCachedSegments() throws IOException;

  /**
   * Store a segment info file for the supplied segment on disk. This operation is idempotent when called multiple
   * times for a given segment.
   */
  void storeInfoFile(DataSegment segment) throws IOException;

  /**
   * Remove the segment info file for the supplied segment from disk. If the file cannot be deleted, do nothing.
   *
   * @see SegmentCacheManager#drop(DataSegment)
   */
  void removeInfoFile(DataSegment segment);


  /**
   * Given a {@link DataSegment}, which contains the instructions for where and how to fetch a
   * {@link Segment} from deep storage, this method returns a
   * {@link ReferenceCountedSegmentProvider}, which manages the lifecycle for the
   * {@link Segment}, allowing callers to check out reference, perform actions
   * using the segment and release the reference when done, ensuring that storage management cannot drop it while in
   * use.
   *
   * @param segment Segment to get on each download (after service bootstrap)
   * @throws SegmentLoadingException If there is an error in loading the segment
   * @see SegmentCacheManager#bootstrap(DataSegment, SegmentLazyLoadFailCallback)
   */
  boolean load(DataSegment segment) throws SegmentLoadingException;

  /**
   * Similar to {@link #load(DataSegment)}, this method returns a {@link ReferenceCountedSegmentProvider} during
   * startup on data nodes.
   *
   * @param segment    Segment to retrieve during service bootstrap
   * @param loadFailed Callback to execute when segment lazy load failed. This applies only when
   *                   {@code lazyLoadOnStart} is enabled
   * @throws SegmentLoadingException - If there is an error in loading the segment
   * @see SegmentCacheManager#load(DataSegment)
   */
  boolean bootstrap(DataSegment segment, SegmentLazyLoadFailCallback loadFailed) throws SegmentLoadingException;

  /**
   * Cleanup the segment files cache space used by the segment, releasing the {@link StorageLocation} reservation
   *
   * @see SegmentCacheManager#removeInfoFile(DataSegment)
   */
  void drop(DataSegment segment);

  SegmentMapAction mapSegment(
      DataSegment dataSegment,
      SegmentDescriptor descriptor,
      SegmentMapFunction segmentMapFunction
  ) throws SegmentLoadingException;

  Optional<Segment> mapSegment(DataSegment dataSegment, SegmentMapFunction segmentMapFunction);

  /**
   * Alternative to {@link #load(DataSegment)}, to return the {@link File} location of the segment files instead
   * of a {@link ReferenceCountedSegmentProvider}.
   * <p>
   * todo (clint): should this just be moved to a different interface now that is only used in production in a single
   *  place? (druid segment input entity for ingest)
   *
   * @throws SegmentLoadingException if there is an error in downloading files
   */
  @Nullable
  File getSegmentFiles(DataSegment segment) throws SegmentLoadingException;

  /**
   * Shutdown any previously set-up bootstrap executor to save resources. This should be called after loading bootstrap
   * segments.
   */
  void shutdownBootstrap();
}
