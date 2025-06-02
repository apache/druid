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
import org.apache.druid.segment.ReferenceCountedObjectProvider;
import org.apache.druid.segment.ReferenceCountedSegmentProvider;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentMapFunction;

import java.util.Optional;

/**
 * Wrapper for a {@link SegmentDescriptor} and a possible {@link Segment} reference acquired by applying a
 * {@link org.apache.druid.segment.SegmentMapFunction} (or directly) to the {@link ReferenceCountedSegmentProvider}
 * retrieved from a {@link org.apache.druid.timeline.VersionedIntervalTimeline} with the descriptor.
 *
 * If the {@link #segmentReference} is empty, it means that either the {@link ReferenceCountedSegmentProvider} was not
 * present in the timeline (e.g. the segment was dropped prior to fetching providers from the timeline) or that a
 * reference could not be acquired from
 * {@link #acquireReference(SegmentDescriptor, ReferenceCountedObjectProvider, SegmentMapFunction, Closer)}, or if
 * built externally, then from {@link SegmentMapFunction} or {@link ReferenceCountedSegmentProvider#acquireReference()}.
 */
public class SegmentReferenceAndDescriptor
{
  /**
   * Creates an 'empty' {@link SegmentReferenceAndDescriptor} which contains no {@link Segment}
   */
  public static SegmentReferenceAndDescriptor empty(SegmentDescriptor descriptor)
  {
    return new SegmentReferenceAndDescriptor(descriptor, Optional.empty());
  }

  /**
   * Builds a {@link SegmentReferenceAndDescriptor} by applying a {@link SegmentMapFunction} to a
   * {@link ReferenceCountedSegmentProvider}, registering the resulting segment, if present, with the {@link Closer}.
   */
  public static SegmentReferenceAndDescriptor acquireReference(
      SegmentDescriptor descriptor,
      ReferenceCountedObjectProvider<Segment> provider,
      SegmentMapFunction mapFunction,
      Closer closer
  )
  {
    return new SegmentReferenceAndDescriptor(descriptor, mapFunction.apply(provider).map(closer::register));
  }

  private final SegmentDescriptor descriptor;
  private final Optional<Segment> segmentReference;

  public SegmentReferenceAndDescriptor(
      SegmentDescriptor descriptor,
      Optional<Segment> segmentReference
  )
  {
    this.segmentReference = segmentReference;
    this.descriptor = descriptor;
  }

  public SegmentDescriptor getDescriptor()
  {
    return descriptor;
  }

  public Optional<Segment> getReference()
  {
    return segmentReference;
  }
}
