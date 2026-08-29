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
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.error.DruidException;
import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.loading.AcquireMode;
import org.apache.druid.segment.loading.AcquireSegmentAction;
import org.apache.druid.segment.loading.AcquireSegmentResult;
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
   * DataSegment fetched from the Coordinator by the deferred acquire chain. Written before the inner (stage 2)
   * acquire starts, so it is always visible to {@link #countDelivered} for a delivered deferred acquire.
   */
  @Nullable
  private volatile DataSegment fetchedDataSegment;

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
   * @param inputCounters     optional counters for tracking input, updated at delivery via {@link #countDelivered}
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
  public String description()
  {
    return segmentId.toString();
  }

  @Override
  public synchronized Optional<Segment> acquireIfCached(AcquireMode acquireMode)
  {
    if (acquired) {
      throw DruidException.defensive("Segment with descriptor[%s] is already acquired", descriptor);
    }

    final Optional<Segment> cachedSegment = segmentManager.acquireCachedSegment(segmentId, acquireMode);
    if (cachedSegment.isPresent()) {
      acquired = true;

      // Update counters inline; the countDelivered hook is only for the acquire() path.
      if (inputCounters != null) {
        final int rowCount = LoadableSegmentUtils.getSegmentRowCount(cachedSegment.get());
        final long byteCount = cachedDataSegment != null ? cachedDataSegment.getSize() : 0;
        inputCounters.addFile(rowCount, byteCount);
      }
    }
    return cachedSegment;
  }

  @Override
  public synchronized AcquireSegmentAction acquire(AcquireMode acquireMode)
  {
    if (acquired) {
      throw DruidException.defensive("Segment with descriptor[%s] is already acquired", descriptor);
    }

    acquired = true;

    if (cachedDataSegment != null) {
      // The SegmentManager handle is already the right shape; counter updates happen at delivery via countDelivered.
      return segmentManager.acquireSegment(cachedDataSegment, acquireMode);
    } else {
      // We can't acquire from the SegmentManager yet because we don't have the DataSegment object; it needs to be
      // fetched from the Coordinator first. Hand-rolled two-stage chain: an outer handle whose canceler tears down
      // whichever stage is active, a DataSegment-future callback that starts the inner (cache manager) acquire under
      // a state guard, and a ready-callback that transfers the inner result to the outer handle.
      final DeferredAcquireState state = new DeferredAcquireState();
      final ListenableFuture<DataSegment> dsFuture = dataSegmentFutureSupplier.get();
      final AcquireSegmentAction outer = new AcquireSegmentAction(() -> {
        final Runnable closeInner;
        synchronized (state) {
          state.closed = true;
          closeInner = state.closeInnerOnce;
        }
        // Cancelling the memoized DataSegment future poisons later dataSegmentFuture() calls, which is safe today:
        // acquire() is once-only and there are no other production consumers of dataSegmentFuture() after acquire.
        dsFuture.cancel(true);
        if (closeInner != null) {
          closeInner.run();
        }
      });
      Futures.addCallback(
          dsFuture,
          new FutureCallback<>()
          {
            @Override
            public void onSuccess(DataSegment dataSegment)
            {
              fetchedDataSegment = dataSegment;
              final AcquireSegmentAction inner;
              final Runnable closeInnerOnce;
              try {
                synchronized (state) {
                  if (state.closed) {
                    // outer was closed before stage 2 could start; nothing acquired, nothing to clean up
                    return;
                  }
                }
                // Acquire OUTSIDE the state lock: acquireSegment can do real work (storage-location reservation,
                // eviction, info-file writes), and the outer canceler takes the state lock — which a closer (e.g.
                // ReadableInputQueue.close) may drive — so holding it here would stall cancellation behind
                // deep-storage-side work. acquireSegment can also throw synchronously (e.g. CAPACITY_EXCEEDED, or a
                // rejected load-pool submit); a throw escaping this callback would be swallowed by the direct
                // executor, leaving outer never delivered and its consumer hung — route it to outer.setException.
                inner = segmentManager.acquireSegment(dataSegment, acquireMode);
              }
              catch (Throwable t) {
                outer.setException(t);
                return;
              }
              closeInnerOnce = AcquireSegmentHandles.closeOnce(inner::close);
              final boolean closedWhileAcquiring;
              synchronized (state) {
                closedWhileAcquiring = state.closed;
                if (!closedWhileAcquiring) {
                  state.closeInnerOnce = closeInnerOnce;
                }
              }
              if (closedWhileAcquiring) {
                // the outer handle was closed while we were acquiring; the canceler ran before closeInnerOnce was
                // published, so we own the freshly-acquired inner handle and must close it ourselves
                closeInnerOnce.run();
                return;
              }
              // outside the state lock: ready callbacks can fire immediately on the registering thread
              AcquireSegmentHandles.transferOnReady(inner, outer, closeInnerOnce);
            }

            @Override
            public void onFailure(Throwable t)
            {
              // silently absorbed if the outer handle was closed first
              outer.setException(t);
            }
          },
          MoreExecutors.directExecutor()
      );
      return outer;
    }
  }

  @Override
  public void countDelivered(AcquireSegmentResult result)
  {
    if (inputCounters == null) {
      return;
    }
    inputCounters.addLoad(result);
    final int rowCount = result.getSegment().map(LoadableSegmentUtils::getSegmentRowCount).orElse(0);
    final DataSegment sizeSource = cachedDataSegment != null ? cachedDataSegment : fetchedDataSegment;
    // Parity with the old countedLoad: addFile fires even for an empty delivery, with rowCount 0.
    inputCounters.addFile(rowCount, sizeSource == null ? 0 : sizeSource.getSize());
  }

  /**
   * Stage guard for the deferred acquire chain: serializes "outer closed" against "stage 2 started" so the inner
   * handle is either never created, or is closed exactly once (by the canceler or by the transfer's failure path,
   * whichever comes first — both go through {@link #closeInnerOnce}).
   */
  private static final class DeferredAcquireState
  {
    @GuardedBy("this")
    boolean closed;
    @GuardedBy("this")
    @Nullable
    Runnable closeInnerOnce;
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
    return DruidException.forPersona(DruidException.Persona.OPERATOR)
                         .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                         .build("Segment[%s] not found on this server. Please retry your query.", segmentId);
  }
}
