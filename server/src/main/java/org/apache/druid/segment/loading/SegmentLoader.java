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

/**
 * Loading segments from deep storage to local storage. Internally, this class can delegate the download to
 * {@link SegmentCacheManager}. Implementations must be thread-safe.
 */
public interface SegmentLoader
{
  /**
   * Builds a {@link Segment} by downloading if necessary
   * @param segment - Segment to load
   * @param lazy - Whether column metadata de-serialization is to be deferred to access time. Setting this flag to true can speed up segment loading
   * @param loadFailed - Callback to invoke if lazy loading fails during column access.
   * @throws SegmentLoadingException - If there is an error in loading the segment
   */
  Segment getSegment(DataSegment segment, boolean lazy, SegmentLazyLoadFailCallback loadFailed) throws SegmentLoadingException;

  /**
   * cleanup any state used by this segment
   */
  void cleanup(DataSegment segment);
}
