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
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.segment.loading.AcquireSegmentAction;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;

/**
 * Wrapper for a {@link SegmentDescriptor} and {@link Optional<Segment>}, the latter being created by a
 * {@link SegmentMapFunction} being applied to a {@link ReferenceCountedSegmentProvider}. Closing this object closes
 * both the {@link #segmentReference} and any closeables attached from the process of creating this object, such as
 * from {@link AcquireSegmentAction}
 */
public class SegmentReference implements Closeable
{
  public static SegmentReference missing(SegmentDescriptor segmentDescriptor)
  {
    return new SegmentReference(segmentDescriptor, Optional.empty(), AcquireSegmentAction.NOOP_CLEANUP);
  }

  private final SegmentDescriptor segmentDescriptor;
  private final Optional<Segment> segmentReference;
  private final Closer closer = Closer.create();

  public SegmentReference(
      SegmentDescriptor segmentDescriptor,
      Optional<Segment> segmentReference,
      Closeable cleanupHold
  )
  {
    this.segmentDescriptor = segmentDescriptor;
    closer.register(cleanupHold);
    this.segmentReference = segmentReference.map(closer::register);
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
