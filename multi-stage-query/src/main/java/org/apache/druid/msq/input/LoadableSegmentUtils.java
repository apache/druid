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

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.segment.PhysicalSegmentInspector;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.loading.AcquireSegmentAction;
import org.apache.druid.segment.loading.AcquireSegmentResult;

import javax.annotation.Nullable;
import java.util.Optional;

public class LoadableSegmentUtils
{
  /**
   * Given a future from {@link AcquireSegmentAction#getSegmentFuture()}, wraps it with logic to increment
   * counters as follows:
   *
   * <ul>
   *   <li>{@link ChannelCounters#addLoad(AcquireSegmentResult)} when the load completes</li>
   *   <li>{@link ChannelCounters#addFile(long, long)} each time a reference is acquired from
   *   {@link AcquireSegmentResult#getReferenceProvider()}. The row count is taken from
   *   {@link #getSegmentRowCount(Segment)}, and byte count is taken from {@code byteCount}.</li>
   * </ul>
   */
  public static ListenableFuture<AcquireSegmentResult> countedLoad(
      final ListenableFuture<AcquireSegmentResult> segmentFuture,
      final long byteCount,
      @Nullable final ChannelCounters channelCounters
  )
  {
    if (channelCounters == null) {
      return segmentFuture;
    } else {
      return FutureUtils.transform(
          segmentFuture,
          result -> {
            channelCounters.addLoad(result);
            return new AcquireSegmentResult(
                () -> {
                  final Optional<Segment> segment = result.getReferenceProvider().acquireReference();
                  final int rowCount = segment.map(LoadableSegmentUtils::getSegmentRowCount).orElse(0);
                  channelCounters.addFile(rowCount, byteCount);
                  return segment;
                },
                result.getLoadSizeBytes(),
                result.getWaitTimeNanos(),
                result.getLoadTimeNanos()
            );
          }
      );
    }
  }

  /**
   * Gets the number of rows for a segment, using a {@link PhysicalSegmentInspector}. Returns 0 when unknown.
   */
  public static int getSegmentRowCount(final Segment segment)
  {
    final PhysicalSegmentInspector inspector = segment.as(PhysicalSegmentInspector.class);
    return inspector != null ? inspector.getNumRows() : 0;
  }
}
