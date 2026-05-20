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

package org.apache.druid.msq.querykit;

import com.google.common.base.Preconditions;
import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.segment.SegmentReference;

import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Holds a {@link SegmentReference} and ensures it is only retrieved one time. Whoever successfully retrieves it
 * from {@link #getSegmentReferenceOnce()} is responsible for closing it.
 */
public class SegmentReferenceHolder
{
  private final AtomicReference<SegmentReference> segmentReference = new AtomicReference<>();
  @Nullable
  private final ChannelCounters inputCounters;
  @Nullable
  private final String description;
  private final SegmentDescriptor descriptor;

  public SegmentReferenceHolder(
      SegmentReference segmentReference,
      @Nullable ChannelCounters inputCounters,
      @Nullable String description
  )
  {
    this.segmentReference.set(Preconditions.checkNotNull(segmentReference, "segmentReference"));
    this.descriptor = Preconditions.checkNotNull(segmentReference, "segmentReference").getSegmentDescriptor();
    this.inputCounters = inputCounters;
    this.description = description;
  }

  /**
   * Called by the caller to return the {@link SegmentReference}. Works only one time. Subsequent calls return null.
   * If this method returns nonnull, the caller is responsible for closing the returned {@link SegmentReference}.
   */
  @Nullable
  public SegmentReference getSegmentReferenceOnce()
  {
    return segmentReference.getAndSet(null);
  }

  /**
   * Same as the descriptor from {@link #getSegmentReferenceOnce()}, except this method returns nonnull even if
   * {@link #getSegmentReferenceOnce()} returns null.
   */
  public SegmentDescriptor getDescriptor()
  {
    return descriptor;
  }

  /**
   * Input counters that should be incremented as we read, or null if none should be incremented.
   */
  @Nullable
  public ChannelCounters getInputCounters()
  {
    return inputCounters;
  }

  /**
   * User-oriented description, suitable for inclusion in error messages.
   */
  @Nullable
  public String description()
  {
    return description;
  }
}
