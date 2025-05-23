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

import java.util.Optional;
import java.util.function.Function;

/**
 * Functional interface that captures the process of acquiring a {@link Segment} from a
 * {@link ReferenceCountedObjectProvider<Segment>} and performing any transformations done on top of this leaf {@link Segment}
 * before it is available to query processing engines.
 * <p>
 * The {@link Segment} returned by this method, if present, must always be closed to return the reference to the base
 * {@link ReferenceCountedObjectProvider<Segment>} that it came from.
 */
@FunctionalInterface
public interface SegmentMapFunction extends Function<ReferenceCountedObjectProvider<Segment>, Optional<Segment>>
{
  /**
   * {@link SegmentMapFunction} that acquires a {@link Segment} from the {@link ReferenceCountedObjectProvider<Segment>}
   */
  SegmentMapFunction IDENTITY = ReferenceCountedObjectProvider::acquireReference;

  /**
   * Returns a {@link SegmentMapFunction} that calls {@link ReferenceCountedObjectProvider#acquireReference()} and then
   * applies the function to the resulting {@link Segment}, if present
   */
  static SegmentMapFunction map(Function<Segment, Segment> mapFn)
  {
    return segmentReference -> segmentReference.acquireReference().map(mapFn);
  }

  /**
   * Returns a {@link SegmentMapFunction} which first applies this {@link SegmentMapFunction} and then applies
   * the function to the resulting {@link Segment}, if present
   */
  default SegmentMapFunction thenMap(Function<Segment, Segment> mapFn)
  {
    return segmentReference -> apply(segmentReference).map(mapFn);
  }
}
