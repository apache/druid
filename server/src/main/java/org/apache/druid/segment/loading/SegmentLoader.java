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

import org.apache.druid.guice.annotations.ExtensionPoint;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.SegmentLazyLoadFailCallback;
import org.apache.druid.timeline.DataSegment;

import java.io.File;

/**
 * Loading segments from deep storage to local storage.
 * Implementations must be thread-safe.
 */
@ExtensionPoint
public interface SegmentLoader
{
  boolean isSegmentLoaded(DataSegment segment);

  /**
   * Returns a {@link ReferenceCountingSegment} that will be added by the {@link org.apache.druid.server.SegmentManager}
   * to the {@link org.apache.druid.timeline.VersionedIntervalTimeline}. This method can be called multiple times
   * by the {@link org.apache.druid.server.SegmentManager}
   * @param segment - {@link DataSegment} segment to download
   * @param lazy - whether the loading is lazy
   * @param loadFailed - Callback to invoke if the loading fails
   * @return
   * @throws SegmentLoadingException
   */
  ReferenceCountingSegment getSegment(DataSegment segment, boolean lazy, SegmentLazyLoadFailCallback loadFailed) throws SegmentLoadingException;

  File getSegmentFiles(DataSegment segment) throws SegmentLoadingException;

  void cleanup(DataSegment segment);
}
