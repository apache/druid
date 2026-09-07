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

package org.apache.druid.query;

import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.utils.CloseableUtils;

import java.util.ArrayList;

/**
 * Holder for decomposing a list of {@link DataSegmentAndDescriptor} into a list of 'active' {@link SegmentReference}
 * for any segments which are already present in the segments cache, and the remaining list of
 * {@link DataSegmentAndDescriptor} which must still be loaded on demand as well as any 'missing'
 * {@link SegmentDescriptor} which are unavailable to load.
 */
public class LeafSegmentsBundle
{
  private final ArrayList<SegmentReference> cachedSegments;
  private final ArrayList<DataSegmentAndDescriptor> loadableSegments;
  private final ArrayList<SegmentDescriptor> missingSegments;

  public LeafSegmentsBundle(
      ArrayList<SegmentReference> cachedSegments,
      ArrayList<DataSegmentAndDescriptor> loadableSegments,
      ArrayList<SegmentDescriptor> missingSegments
  )
  {
    this.cachedSegments = cachedSegments;
    this.missingSegments = missingSegments;
    this.loadableSegments = loadableSegments;
  }

  /**
   * List of {@link SegmentReference} for segments which are already loaded in the segment cache. These must be closed
   * when the caller is finished with them.
   */
  public ArrayList<SegmentReference> getCachedSegments()
  {
    return cachedSegments;
  }

  /**
   * List of {@link DataSegmentAndDescriptor} for segments which are not current present in the segment cache. These
   * must be loaded into the cache
   */
  public ArrayList<DataSegmentAndDescriptor> getLoadableSegments()
  {
    return loadableSegments;
  }

  /**
   * List of {@link SegmentDescriptor} which are not available in the cache (and cannot be downloaded because on
   * demand download is not enabled or a {@link DataSegmentAndDescriptor} was unavailable)
   */
  public ArrayList<SegmentDescriptor> getMissingSegments()
  {
    return missingSegments;
  }

  public void closeCachedReferences()
  {
    final Closer closer = CloseableUtils.forIterable(cachedSegments);
    CloseableUtils.closeAndWrapExceptions(closer);
  }
}
