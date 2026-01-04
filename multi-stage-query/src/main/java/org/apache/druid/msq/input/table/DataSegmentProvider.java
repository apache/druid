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

package org.apache.druid.msq.input.table;

import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.msq.input.LoadableSegment;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.timeline.SegmentId;

import javax.annotation.Nullable;

public interface DataSegmentProvider
{
  /**
   * Returns a {@link LoadableSegment} that can fetch the segment corresponding to the provided segmentId.
   *
   * @param segmentId     segment ID to fetch
   * @param descriptor    segment descriptor for querying. It is possible for this interval to be more restricted
   *                      than the interval from {@link SegmentId#getInterval()}.
   * @param inputCounters counters to pass through to {@link LoadableSegment#inputCounters()} in the returned object
   * @param isReindex     true if this is a DML command (INSERT or REPLACE) writing into the same table it is
   *                      reading from; false otherwise. When true, implementations must only allow reading from
   *                      segments that are currently-used according to the Coordinator.
   */
  LoadableSegment getLoadableSegment(
      SegmentId segmentId,
      SegmentDescriptor descriptor,
      @Nullable ChannelCounters inputCounters,
      boolean isReindex
  );
}
