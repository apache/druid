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

package org.apache.druid.msq.dart.worker;

import com.google.inject.Inject;
import org.apache.druid.collections.ReferenceCountingResourceHolder;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.msq.querykit.DataSegmentProvider;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.segment.CompleteSegment;
import org.apache.druid.segment.PhysicalSegmentInspector;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;

import java.io.Closeable;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Implementation of {@link DataSegmentProvider} that uses locally-cached segments from a {@link SegmentManager}.
 */
public class DartDataSegmentProvider implements DataSegmentProvider
{
  private final SegmentManager segmentManager;

  @Inject
  public DartDataSegmentProvider(SegmentManager segmentManager)
  {
    this.segmentManager = segmentManager;
  }

  @Override
  public Supplier<ResourceHolder<CompleteSegment>> fetchSegment(
      SegmentId segmentId,
      ChannelCounters channelCounters,
      boolean isReindex
  )
  {
    if (isReindex) {
      throw DruidException.defensive("Got isReindex[%s], expected false", isReindex);
    }

    return () -> {
      final Optional<VersionedIntervalTimeline<String, ReferenceCountingSegment>> timeline =
          segmentManager.getTimeline(new TableDataSource(segmentId.getDataSource()).getAnalysis());

      if (!timeline.isPresent()) {
        throw segmentNotFound(segmentId);
      }

      final PartitionChunk<ReferenceCountingSegment> chunk =
          timeline.get().findChunk(
              segmentId.getInterval(),
              segmentId.getVersion(),
              segmentId.getPartitionNum()
          );

      if (chunk == null) {
        throw segmentNotFound(segmentId);
      }

      final ReferenceCountingSegment segment = chunk.getObject();
      final Optional<Closeable> closeable = segment.acquireReferences();
      if (!closeable.isPresent()) {
        // Segment has disappeared before we could acquire a reference to it.
        throw segmentNotFound(segmentId);
      }

      final Closer closer = Closer.create();
      closer.register(closeable.get());
      closer.register(() -> {
        final PhysicalSegmentInspector inspector = segment.as(PhysicalSegmentInspector.class);
        channelCounters.addFile(inspector != null ? inspector.getNumRows() : 0, 0);
      });
      return new ReferenceCountingResourceHolder<>(new CompleteSegment(null, segment), closer);
    };
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
