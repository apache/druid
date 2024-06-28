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

package org.apache.druid.segment.realtime;


import org.apache.druid.segment.SegmentReference;
import org.apache.druid.utils.CloseableUtils;

import java.io.Closeable;
import java.util.function.Function;

/**
 * Segment reference for a {@link PartialSegment} returned by
 * {@link AppendableSegment#acquireSegmentReferences(Function, boolean)}. Must be closed in order to release the
 * reference.
 */
public class PartialSegmentReference implements Closeable
{
  private final int partialSegmentNumber;
  private final SegmentReference segment;
  private final boolean immutable;
  private final Closeable releaser;

  public PartialSegmentReference(
      int partialSegmentNumber,
      SegmentReference segment,
      boolean immutable,
      Closeable releaser
  )
  {
    this.partialSegmentNumber = partialSegmentNumber;
    this.segment = segment;
    this.immutable = immutable;
    this.releaser = releaser;
  }

  /**
   * Index of the {@link PartialSegment} within the {@link AppendableSegment} that this segment
   * reference came from.
   */
  public int getPartialSegmentNumber()
  {
    return partialSegmentNumber;
  }

  /**
   * The segment reference.
   */
  public SegmentReference getSegment()
  {
    return segment;
  }

  /**
   * Whether the segment is immutable.
   */
  public boolean isImmutable()
  {
    return immutable;
  }

  @Override
  public void close()
  {
    CloseableUtils.closeAndWrapExceptions(releaser);
  }
}
