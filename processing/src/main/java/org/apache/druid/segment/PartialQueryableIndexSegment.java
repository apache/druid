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

package org.apache.druid.segment;

import org.apache.druid.timeline.SegmentId;
import org.apache.druid.utils.CloseableUtils;
import org.joda.time.Interval;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Closeable;

/**
 * {@link Segment} wrapper around a {@link PartialQueryableIndex}. Mirrors {@link QueryableIndexSegment} but wires up
 * the V10-specific {@link V10TimeBoundaryInspector} (which answers from
 * {@link org.apache.druid.segment.projections.ProjectionMetadata} min/max fields without downloading the
 * {@code __time} column) and the partial-aware {@link PartialQueryableIndexCursorFactory} (which downloads
 * required files on the supplied download executor before handing back a cursor).
 * <p>
 * Lifecycle: this segment is intended to exist as a transient reference-hold scope over an externally-owned
 * {@link PartialQueryableIndex}, it never closes the underlying index. The {@code onClose} hook is what
 * {@link #close()} fires when the segment is closed (e.g. to 'release' reference tracking stuff in the cache layer).
 */
public class PartialQueryableIndexSegment implements ReferenceCountedSegmentProvider.LeafReference
{
  private final PartialQueryableIndex index;
  private final PartialQueryableIndexCursorFactory cursorFactory;
  private final TimeBoundaryInspector timeBoundaryInspector;
  private final SegmentId segmentId;
  private final Closeable onClose;

  public PartialQueryableIndexSegment(
      PartialQueryableIndex index,
      SegmentId segmentId,
      Closeable onClose,
      PartialBundleAcquirer bundleAcquirer
  )
  {
    this.index = index;
    this.timeBoundaryInspector = V10TimeBoundaryInspector.forBaseProjection(
        index.getBaseProjectionMetadata(),
        index.getDataInterval()
    );
    this.cursorFactory = new PartialQueryableIndexCursorFactory(
        index,
        timeBoundaryInspector,
        bundleAcquirer
    );
    this.segmentId = segmentId;
    this.onClose = onClose;
  }

  @Override
  public SegmentId getId()
  {
    return segmentId;
  }

  @Override
  public Interval getDataInterval()
  {
    return index.getDataInterval();
  }

  @Override
  public void close()
  {
    CloseableUtils.closeAndWrapExceptions(onClose);
  }

  @SuppressWarnings("unchecked")
  @Nullable
  @Override
  public <T> T as(@Nonnull Class<T> clazz)
  {
    if (CursorFactory.class.equals(clazz)) {
      return (T) cursorFactory;
    } else if (QueryableIndex.class.equals(clazz)) {
      return (T) index;
    } else if (TimeBoundaryInspector.class.equals(clazz)) {
      return (T) timeBoundaryInspector;
    } else if (Metadata.class.equals(clazz)) {
      return (T) index.getMetadata();
    }
    return null;
  }

  @Override
  public String getDebugString()
  {
    return getClass().getSimpleName() + ":" + segmentId;
  }
}
