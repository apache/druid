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

import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.DataSegmentAndDescriptor;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.segment.loading.AcquireSegmentAction;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;

/**
 * Wrapper for a {@link DataSegment} (Nullable), a {@link SegmentDescriptor} and an optional {@link Segment}.
 * {@link Segment} is created by a {@link SegmentMapFunction} being applied to a {@link ReferenceCountedSegmentProvider}.
 *
 * <p>
 * Closing this object closes both the {@link #segmentReference} and any closeables attached from the process of
 * creating this object, such as from {@link AcquireSegmentAction}.
 */
public class SegmentReference implements Closeable
{
  public static SegmentReference missing(SegmentDescriptor segmentDescriptor)
  {
    return new SegmentReference(segmentDescriptor, Optional.empty(), null);
  }

  @Nullable
  private final DataSegment dataSegment;
  private final SegmentDescriptor segmentDescriptor;
  private final Optional<Segment> segmentReference;
  private final Closer closer = Closer.create();

  public SegmentReference(
      DataSegmentAndDescriptor segment,
      Optional<Segment> segmentReference,
      @Nullable Closeable cleanupHold
  )
  {
    this(segment.getDataSegment(), segment.getDescriptor(), segmentReference, cleanupHold);
  }

  public SegmentReference(
      SegmentDescriptor segmentDescriptor,
      Optional<Segment> segmentReference,
      @Nullable Closeable cleanupHold
  )
  {
    this(null, segmentDescriptor, segmentReference, cleanupHold);
  }

  private SegmentReference(
      @Nullable DataSegment segment,
      SegmentDescriptor segmentDescriptor,
      Optional<Segment> segmentReference,
      @Nullable Closeable cleanupHold
  )
  {
    this.dataSegment = segment;
    this.segmentDescriptor = segmentDescriptor;
    closer.register(cleanupHold);
    this.segmentReference = segmentReference.map(closer::register);
  }

  @Nullable
  public DataSegment getDataSegment()
  {
    return dataSegment;
  }

  public SegmentDescriptor getSegmentDescriptor()
  {
    return segmentDescriptor;
  }

  public Optional<Segment> getSegmentReference()
  {
    return segmentReference;
  }

  @Override
  public void close() throws IOException
  {
    closer.close();
  }
}
