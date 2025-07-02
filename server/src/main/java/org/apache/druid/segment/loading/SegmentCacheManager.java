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

import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentLazyLoadFailCallback;
import org.apache.druid.segment.SegmentMapFunction;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
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
   * Given a {@link DataSegment}, which contains the instructions for where and how to fetch a {@link Segment} from
   * deep storage, this method tries to load and subsequently serve it to callers via
   * {@link #acquireSegment(DataSegment)} or {@link #acquireSegment(DataSegment, SegmentDescriptor)}. If the segment
   * cannot be loaded either due to error or insufficient storage space, this method throws a
   * {@link SegmentLoadingException}.
   *
   * @param segment Segment to get on each download (after service bootstrap)
   * @throws SegmentLoadingException If there is an error in loading the segment or insufficient storage space
   * @see SegmentCacheManager#bootstrap(DataSegment, SegmentLazyLoadFailCallback)
   */
  void load(DataSegment segment) throws SegmentLoadingException;

  /**
   * Similar to {@link #load(DataSegment)}, this method loads segments during startup on data nodes. Implementations of
   * this method may be configured to use a larger than normal work pool that only exists during startup and is shutdown
   * after startup by calling {@link #shutdownBootstrap()}
   *
   * @param segment    Segment to retrieve during service bootstrap
   * @param loadFailed Callback to execute when segment lazy load failed. This applies only when
   *                   {@code lazyLoadOnStart} is enabled
   * @throws SegmentLoadingException - If there is an error in loading the segment or insufficient storage space
   * @see SegmentCacheManager#load(DataSegment)
   * @see SegmentCacheManager#shutdownBootstrap()
   */
  void bootstrap(DataSegment segment, SegmentLazyLoadFailCallback loadFailed) throws SegmentLoadingException;

  /**
   * Cleanup the segment files cache space used by the segment, releasing the {@link StorageLocation} reservation
   *
   * @see SegmentCacheManager#removeInfoFile(DataSegment)
   */
  void drop(DataSegment segment);

  /**
   * Applies a {@link SegmentMapFunction} to a {@link Segment} if it is available in the cache. If not present in any
   * storage location, this method will not attempt to download the {@link DataSegment} from deep storage. The
   * {@link Segment} returned by this method is considered an open reference, cache implementations must not allow it
   * to be dropped until it has been closed. As such, the returned {@link Segment} must be closed when the caller is
   * finished doing segment things.
   */
  Optional<Segment> acquireSegment(DataSegment dataSegment);

  /**
   * Returns a {@link AcquireSegmentAction} for a given {@link DataSegment} and {@link SegmentDescriptor}, which returns
   * the {@link Segment} if already present in the cache, or tries to fetch from deep storage and map if not. The
   * {@link Segment} returned by this method is considered an open reference, cache implementations must not allow it
   * to be dropped until it has been closed. As such, the returned {@link Segment} must be closed when the caller is
   * finished doing segment things.
   */
  AcquireSegmentAction acquireSegment(
      DataSegment dataSegment,
      SegmentDescriptor descriptor
  ) throws SegmentLoadingException;

  /**
   * Alternative to {@link #acquireSegment(DataSegment)}, to return the {@link File} location of the segment files
   * stored in the cache, instead of a {@link Optional<Segment>}. Unlike {@link #acquireSegment(DataSegment)} and
   * {@link #acquireSegment(DataSegment, SegmentDescriptor)}, this method does not provide any protections for callers,
   * and should only be used by callers that are in control of when {@link #drop(DataSegment)} is called. This method
   * will not download the segment files from deep storage if they do not already exist in the cache, callers should use
   * {@link #load(DataSegment)} before calling this method.
   */
  @Nullable
  File getSegmentFiles(DataSegment segment) throws SegmentLoadingException;

  /**
   * Shutdown any previously set-up bootstrap executor to save resources. This should be called after loading bootstrap
   * segments.
   */
  void shutdownBootstrap();
}
