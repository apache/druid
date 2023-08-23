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

import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.segment.Segment;
import org.apache.druid.timeline.SegmentId;

import java.util.function.Supplier;

public interface DataSegmentProvider
{
  /**
   * Returns a supplier that fetches the segment corresponding to the provided segmentId from deep storage. The segment
   * is not actually fetched until you call {@link Supplier#get()}. Once you call this, make sure to also call
   * {@link ResourceHolder#close()}.
   *
   * It is not necessary to call {@link ResourceHolder#close()} if you never call {@link Supplier#get()}.
   */
  Supplier<ResourceHolder<Segment>> fetchSegment(
      SegmentId segmentId,
      ChannelCounters channelCounters,
      boolean isReindex
  );
}
