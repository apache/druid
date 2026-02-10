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

package org.apache.druid.msq.input;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.loading.AcquireSegmentAction;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;

import javax.annotation.Nullable;
import java.util.Optional;

/**
 * Implementation of {@link LoadableSegment} for regular Druid segments loaded via {@link SegmentManager}.
 * Created by {@link org.apache.druid.msq.input.table.SegmentsInputSliceReader}.
 */
public class RegularLoadableSegment implements LoadableSegment
{
  private final SegmentManager segmentManager;
  private final SegmentId segmentId;
  private final SegmentDescriptor descriptor;
  @Nullable
  private final ChannelCounters inputCounters;
  @Nullable
  private final CoordinatorClient coordinatorClient;
  private final boolean isReindex;

  @GuardedBy("this")
  private boolean acquired;

  /**
   * Cached DataSegment from local timeline, if available. Null if not in local timeline or if isReindex is true.
   */
  @Nullable
  private final DataSegment cachedDataSegment;

  /**
   * Memoized supplier for the DataSegment future.
   */
  private final Supplier<ListenableFuture<DataSegment>> dataSegmentFutureSupplier;

  /**
   * Create a new RegularLoadableSegment.
   *
   * @param segmentManager    segment manager for loading and caching segments
   * @param segmentId         the segment ID to load
   * @param descriptor        segment descriptor for querying
   * @param inputCounters     optional counters for tracking input
   * @param coordinatorClient optional client for fetching DataSegment from Coordinator when not available locally
   * @param isReindex         true if this is a DML command writing to the same table it's reading from
   */
  public RegularLoadableSegment(
      final SegmentManager segmentManager,
      final SegmentId segmentId,
      final SegmentDescriptor descriptor,
      @Nullable final ChannelCounters inputCounters,
      @Nullable final CoordinatorClient coordinatorClient,
      final boolean isReindex
  )
  {
    if (isReindex && coordinatorClient == null) {
      throw DruidException.defensive("Got isReindex[%s], cannot respect this without a coordinatorClient", isReindex);
    }

    this.segmentManager = segmentManager;
    this.segmentId = segmentId;
    this.descriptor = descriptor;
    this.inputCounters = inputCounters;
    this.coordinatorClient = coordinatorClient;
    this.isReindex = isReindex;

    // Can't rely on local timeline if isReindex; always need to check the Coordinator to confirm the segment
    // is still active.
    this.cachedDataSegment = isReindex ? null : getDataSegmentFromLocalTimeline();
    this.dataSegmentFutureSupplier = Suppliers.memoize(this::fetchDataSegment);
  }

  @Override
  public ListenableFuture<DataSegment> dataSegmentFuture()
  {
    return Futures.nonCancellationPropagating(dataSegmentFutureSupplier.get());
  }

  @Override
  public SegmentDescriptor descriptor()
  {
    return descriptor;
  }

  @Override
  @Nullable
  public ChannelCounters inputCounters()
  {
    return inputCounters;
  }

  @Override
  @Nullable
  public String description()
  {
    return segmentId.toString();
  }

  @Override
  public synchronized Optional<Segment> acquireIfCached()
  {
    if (acquired) {
      throw DruidException.defensive("Segment with descriptor[%s] is already acquired", descriptor);
    }

    if (cachedDataSegment != null) {
      final Optional<Segment> cachedSegment = segmentManager.acquireCachedSegment(cachedDataSegment);
      if (cachedSegment.isPresent()) {
        acquired = true;
      }
      return cachedSegment;
    }

    return Optional.empty();
  }

  @Override
  public synchronized AcquireSegmentAction acquire()
  {
    if (acquired) {
      throw DruidException.defensive("Segment with descriptor[%s] is already acquired", descriptor);
    }

    acquired = true;

    if (cachedDataSegment != null) {
      return segmentManager.acquireSegment(cachedDataSegment);
    } else {
      // Create a shim AcquireSegmentAction that doesn't acquire a hold (yet). We can't make a real
      // AcquireSegmentAction yet because we don't have the DataSegment object. It needs to be fetched
      // from the Coordinator. That call is deferred until we're actually ready to load the segment, because
      // we don't make the calls all at once when loading a lot of segments.

      final Closer closer = Closer.create();
      return new AcquireSegmentAction(
          Suppliers.memoize(() -> FutureUtils.transformAsync(
              dataSegmentFutureSupplier.get(),
              dataSegment -> closer.register(segmentManager.acquireSegment(dataSegment)).getSegmentFuture()
          )),
          closer
      );
    }
  }

  /**
   * Fetches the {@link DataSegment}, either returning it immediately if cached or fetching from the Coordinator.
   */
  private ListenableFuture<DataSegment> fetchDataSegment()
  {
    if (cachedDataSegment != null) {
      return Futures.immediateFuture(cachedDataSegment);
    } else if (coordinatorClient != null) {
      return coordinatorClient.fetchSegment(
          segmentId.getDataSource(),
          segmentId.toString(),
          !isReindex
      );
    } else {
      return Futures.immediateFailedFuture(segmentNotFound());
    }
  }

  /**
   * Returns {@link DataSegment} for the segment ID using our local timeline, if present. Otherwise returns null.
   */
  @Nullable
  private DataSegment getDataSegmentFromLocalTimeline()
  {
    final Optional<VersionedIntervalTimeline<String, DataSegment>> timeline =
        segmentManager.getTimeline(new TableDataSource(segmentId.getDataSource()));

    if (timeline.isEmpty()) {
      return null;
    }

    final PartitionChunk<DataSegment> chunk =
        timeline.get().findChunk(
            segmentId.getInterval(),
            segmentId.getVersion(),
            segmentId.getPartitionNum()
        );

    if (chunk == null) {
      return null;
    }

    return chunk.getObject();
  }

  /**
   * Error to throw when a segment that was requested is not found. This can happen due to segment moves, etc.
   */
  private DruidException segmentNotFound()
  {
    return DruidException.forPersona(DruidException.Persona.USER)
                         .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                         .build("Segment[%s] not found on this server. Please retry your query.", segmentId);
  }
}
