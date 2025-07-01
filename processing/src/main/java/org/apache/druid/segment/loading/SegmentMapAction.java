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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.segment.Segment;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;
import java.util.function.Supplier;

public class SegmentMapAction implements Closeable
{
  private static final Closeable NOOP = () -> {};

  public static SegmentMapAction alreadyLoaded(
      final SegmentDescriptor descriptor,
      final Optional<Segment> segment
  )
  {
    return new SegmentMapAction(descriptor, () -> Futures.immediateFuture(segment), NOOP);
  }

  public static SegmentMapAction missingSegment(final SegmentDescriptor descriptor)
  {
    return new SegmentMapAction(descriptor, () -> Futures.immediateFuture(Optional.empty()), NOOP);
  }

  private final SegmentDescriptor segmentDescriptor;
  private final Supplier<ListenableFuture<Optional<Segment>>> segmentFutureSupplier;
  private final Closeable loadCleanup;

  public SegmentMapAction(
      SegmentDescriptor segmentDescriptor,
      Supplier<ListenableFuture<Optional<Segment>>> segmentFutureSupplier,
      Closeable loadCleanup
  )
  {
    this.segmentDescriptor = segmentDescriptor;
    this.segmentFutureSupplier = segmentFutureSupplier;
    this.loadCleanup = loadCleanup;
  }

  public SegmentDescriptor getDescriptor()
  {
    return segmentDescriptor;
  }

  public ListenableFuture<Optional<Segment>> getSegmentFuture()
  {
    return segmentFutureSupplier.get();
  }

  @Override
  public void close() throws IOException
  {
    loadCleanup.close();
  }
}
