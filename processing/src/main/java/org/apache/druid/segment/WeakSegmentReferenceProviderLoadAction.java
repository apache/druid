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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.query.SegmentDescriptor;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Supplier;

/**
 * Wrapper type representing the act of ensuring that a {@link ReferenceCountedSegmentProvider} is present in the
 * segment cache and mapped, ready for processing.
 */
public class WeakSegmentReferenceProviderLoadAction implements Closeable
{
  private static final Closeable NOOP = () -> {};

  public static WeakSegmentReferenceProviderLoadAction alreadyLoaded(
      final SegmentDescriptor descriptor,
      final LeafSegmentReferenceProvider provider
  )
  {
    return new WeakSegmentReferenceProviderLoadAction(descriptor, () -> Futures.immediateFuture(provider), NOOP);
  }

  public static WeakSegmentReferenceProviderLoadAction missingSegment(final SegmentDescriptor descriptor)
  {
    return new WeakSegmentReferenceProviderLoadAction(descriptor, () -> Futures.immediateFuture(null), NOOP);
  }

  private final SegmentDescriptor segmentDescriptor;
  private final Supplier<ListenableFuture<LeafSegmentReferenceProvider>> loadFuture;
  private final Closeable loadCleanup;

  public WeakSegmentReferenceProviderLoadAction(
      SegmentDescriptor segmentDescriptor,
      Supplier<ListenableFuture<LeafSegmentReferenceProvider>> loadFuture,
      Closeable loadCleanup
  )
  {
    this.segmentDescriptor = segmentDescriptor;
    this.loadFuture = loadFuture;
    this.loadCleanup = loadCleanup;
  }

  public SegmentDescriptor getDescriptor()
  {
    return segmentDescriptor;
  }

  public ListenableFuture<LeafSegmentReferenceProvider> getLoadFuture()
  {
    return loadFuture.get();
  }

  @Override
  public void close() throws IOException
  {
    loadCleanup.close();
  }
}
