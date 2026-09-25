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

package org.apache.druid.indexing.seekablestream;

import org.apache.druid.data.input.InputRow;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.ShardSpec;

import java.util.Collection;

/**
 * Accumulates information from the rows a streaming task ingests and, at publish time, stamps each segment with a
 * {@link ShardSpec} derived from that information. This is the pluggable strategy behind {@link StreamingPartitionsSpec}:
 * a new strategy is added by writing a {@link StreamingPartitionsSpec} subtype plus a matching collector, without
 * touching the task runner. The built-in {@code DimensionValueSetCollector} records each segment's observed dimension
 * values so the broker can prune it at query time without waiting for compaction.
 *
 * <p>One instance is created per task run (via {@link StreamingPartitionsSpec#createCollector()}) and shared across all
 * of that task's segments. Implementations <b>must</b> be thread-safe: {@link #collect} and {@link #annotate} can run
 * concurrently for the same {@link SegmentId} (see {@link #annotate}).
 */
public interface StreamingShardSpecCollector
{
  /**
   * Records whatever information this strategy needs from {@code row} for {@code segmentId}. Called on the run-loop
   * thread, once per row successfully added to {@code segmentId}.
   */
  void collect(SegmentId segmentId, InputRow row);

  /**
   * Notifies the collector that {@code segmentIds} were restored from disk across a task restart. Such segments'
   * pre-restart rows are not re-read, so the collected information is incomplete; to avoid wrongly pruning those rows,
   * {@link #annotate} must return a non-pruning shard spec for them. The full restored set is known at once, so it is
   * passed in a single call rather than one segment at a time. Called only at task startup, before the run loop
   * begins; idempotent.
   */
  void onSegmentsRestored(Collection<SegmentId> segmentIds);

  /**
   * Returns {@code segment} stamped with a shard spec derived from the information collected for it. When the segment
   * was restored across a restart (see {@link #onSegmentsRestored}) or nothing was collected for it, this returns a
   * non-pruning fallback shard spec.
   *
   * <p>Implementations <b>must</b> keep the shard-spec class uniform within a time interval: all segments handed to a
   * single publish must share one shard-spec class, or
   * {@link org.apache.druid.segment.realtime.appenderator.SegmentPublisherHelper} rejects the publish. In practice this
   * means returning the same shard-spec type for both the pruning and the non-pruning (fallback) case, differing only
   * in the declared values. Must be safe to call concurrently with {@link #collect}.
   */
  DataSegment annotate(DataSegment segment);

  /**
   * Notifies the collector that {@code segmentId} has been successfully published and handed off, so any per-segment
   * state accumulated for it can be released. Called only from the publish-success callback.
   */
  void onSegmentPublished(SegmentId segmentId);
}
