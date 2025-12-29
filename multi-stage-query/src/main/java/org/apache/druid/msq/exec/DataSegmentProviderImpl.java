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

package org.apache.druid.msq.exec;

import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.msq.input.LoadableSegment;
import org.apache.druid.msq.input.table.DataSegmentProvider;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.segment.loading.AcquireSegmentAction;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.jetbrains.annotations.Nullable;

import java.util.Optional;

/**
 * Standard implementation of {@link DataSegmentProvider} based on {@link SegmentManager}.
 */
public class DataSegmentProviderImpl implements DataSegmentProvider
{
  private final SegmentManager segmentManager;

  @Nullable
  private final CoordinatorClient coordinatorClient;

  /**
   * Create a new instance.
   *
   * @param segmentManager    segment manager to use for caching
   * @param coordinatorClient client for fetching {@link DataSegment} objects from the Coordinator when they are
   *                          not present in the local timeline. If null, {@link #segmentNotFound(SegmentId)} will
   *                          be thrown instead in this case.
   */
  public DataSegmentProviderImpl(SegmentManager segmentManager, @Nullable CoordinatorClient coordinatorClient)
  {
    this.segmentManager = segmentManager;
    this.coordinatorClient = coordinatorClient;
  }

  @Override
  public LoadableSegment fetchSegment(
      final SegmentId segmentId,
      final SegmentDescriptor descriptor,
      @Nullable final ChannelCounters inputCounters,
      final boolean isReindex
  )
  {
    if (isReindex && coordinatorClient == null) {
      throw DruidException.defensive("Got isReindex[%s], cannot respect this without a coordinatorClient", isReindex);
    }

    // Can't rely on local timeline if isReindex; always need to check the Coordinator to confirm the segment
    // is still active.
    final DataSegment cachedDataSegment = isReindex ? null : getDataSegmentFromLocalTimeline(segmentId);

    return new LoadableSegment(
        descriptor,
        inputCounters,
        StringUtils.format("segment[%s]", segmentId),
        () -> {
          final Closer closer = Closer.create();
          // Create a shim AcquireSegmentAction that doesn't acquire a hold (yet). We can't make a real
          // AcquireSegmentAction yet because we don't have the DataSegment object. It needs to be fetched
          // from the Coordinator. That call is deferred until we're actually ready to load the segment, so
          // we don't make them all at once.
          return new AcquireSegmentAction(
              Suppliers.memoize(() -> {
                final ListenableFuture<DataSegment> dataSegmentFuture;

                if (cachedDataSegment != null) {
                  dataSegmentFuture = Futures.immediateFuture(cachedDataSegment);
                } else if (coordinatorClient != null) {
                  dataSegmentFuture = coordinatorClient.fetchSegment(
                      segmentId.getDataSource(),
                      segmentId.toString(),
                      !isReindex
                  );
                } else {
                  dataSegmentFuture = Futures.immediateFailedFuture(segmentNotFound(segmentId));
                }

                return FutureUtils.transformAsync(
                    dataSegmentFuture,
                    dataSegment -> closer.register(segmentManager.acquireSegment(dataSegment)).getSegmentFuture()
                );
              }),
              closer
          );
        },
        cachedDataSegment != null
    );
  }

  /**
   * Returns {@link DataSegment} for a {@link SegmentId} using our local timeline, if present. Otherwise returns null.
   */
  @Nullable
  private DataSegment getDataSegmentFromLocalTimeline(final SegmentId segmentId)
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
  private static DruidException segmentNotFound(final SegmentId segmentId)
  {
    return DruidException.forPersona(DruidException.Persona.USER)
                         .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                         .build("Segment[%s] not found on this server. Please retry your query.", segmentId);
  }
}
