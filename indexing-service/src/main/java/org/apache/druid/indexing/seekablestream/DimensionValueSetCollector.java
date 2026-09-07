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

import com.google.common.collect.Sets;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.DimensionValueSetShardSpec;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@link StreamingShardSpecCollector} for {@link DimensionValueSetPartitionsSpec}: it records, per segment, the
 * distinct values observed for each configured dimension and stamps each segment with a
 * {@link DimensionValueSetShardSpec} at publish time so the broker can prune it.
 *
 * <p>A {@code null} element denotes an observed null/missing value (kept distinct from {@code ""}) so that
 * {@code IS NULL} queries are not pruned.
 *
 * <p>Thread-safety follows the {@link StreamingShardSpecCollector} contract:
 * {@link #observedPartitionDimValuesBySegment} is a
 * {@link ConcurrentHashMap} keyed by {@link SegmentId} whose value sets are {@link Collections#synchronizedSet} written
 * by the run loop ({@link #collect}) and snapshotted under their own monitor by the publish path ({@link #annotate}).
 */
public class DimensionValueSetCollector implements StreamingShardSpecCollector
{
  private static final Logger log = new Logger(DimensionValueSetCollector.class);

  private final List<String> partitionDimensions;

  /**
   * If non-null, the maximum number of distinct values to record for a single dimension on a single segment. A segment
   * whose observed values for a dimension exceed this cap omits that dimension from its stamped filter map (pruning
   * disabled for it on that segment), guarding against shard-spec bloat.
   */
  @Nullable
  private final Integer maxValuesPerDimension;

  /**
   * Observed values per tracked dimension, keyed by segment identifier. Inner sets permit null and are written by the
   * run loop / read by the publish path under their own monitor. Entries are removed on successful publish via
   * {@link #onSegmentPublished}; a publish failure is terminal for the task, so any remaining entries are reclaimed at
   * task teardown.
   */
  private final ConcurrentHashMap<SegmentId, Map<String, Set<String>>> observedPartitionDimValuesBySegment =
      new ConcurrentHashMap<>();

  /**
   * Segment identifiers restored from disk at startup (i.e. spanning a task restart). Their pre-restart rows are not
   * re-read, so {@link #observedPartitionDimValuesBySegment} would under-include values; to avoid wrongly pruning them,
   * such segments are published with an empty-filter (non-pruning) {@link DimensionValueSetShardSpec} instead of one
   * declaring observed values.
   */
  private final Set<SegmentId> restartSpannedSegments = Sets.newConcurrentHashSet();

  public DimensionValueSetCollector(List<String> partitionDimensions, @Nullable Integer maxValuesPerDimension)
  {
    this.partitionDimensions = partitionDimensions;
    this.maxValuesPerDimension = maxValuesPerDimension;
  }

  @Override
  public void collect(SegmentId segmentId, InputRow row)
  {
    if (partitionDimensions.isEmpty()) {
      return;
    }
    final Map<String, Set<String>> segValues =
        observedPartitionDimValuesBySegment.computeIfAbsent(segmentId, k -> new ConcurrentHashMap<>());
    for (String dim : partitionDimensions) {
      final Set<String> dimSet = segValues.computeIfAbsent(
          dim,
          k -> Collections.synchronizedSet(new HashSet<>())
      );
      // Empty getDimension result means a null/missing value; record null so IS NULL is not pruned
      // (distinct from "", which getDimension returns as [""]).
      final List<String> dimValues = row.getDimension(dim);
      if (dimValues == null || dimValues.isEmpty()) {
        dimSet.add(null);
      } else {
        dimSet.addAll(dimValues);
      }
    }
  }

  @Override
  public void onSegmentsRestored(Collection<SegmentId> segmentIds)
  {
    if (segmentIds.isEmpty()) {
      return;
    }
    restartSpannedSegments.addAll(segmentIds);
    log.warn(
        "Disabling partition-filter pruning for %d segment(s) restored across a task restart: %s",
        segmentIds.size(),
        segmentIds
    );
  }

  /**
   * Stamps a segment with a {@link DimensionValueSetShardSpec} declaring its observed dimension values so the broker
   * can prune it. We always return a {@link DimensionValueSetShardSpec}, falling back to an empty (non-pruning) filter
   * map when values can't be safely declared, so segments in an interval stay class-uniform for
   * {@link org.apache.druid.segment.realtime.appenderator.SegmentPublisherHelper}. A null observed value is carried
   * through (distinct from {@code ""}) so {@code IS NULL} queries are not pruned.
   */
  @Override
  public DataSegment annotate(DataSegment s)
  {
    final Map<String, List<String>> snapshotFilters = new HashMap<>();
    final SegmentId lookupKey = s.getId();
    final Map<String, Set<String>> segObserved = observedPartitionDimValuesBySegment.get(lookupKey);
    // Leave filters empty for restart-spanned segments: their pre-restart values can't be re-observed.
    if (!restartSpannedSegments.contains(lookupKey) && segObserved != null) {
      for (String dim : partitionDimensions) {
        final Set<String> vals = segObserved.get(dim);
        if (vals == null) {
          continue;
        }
        // vals is a synchronized set written by the run loop; copy it under its monitor to iterate safely.
        final List<String> snapshot;
        synchronized (vals) {
          if (vals.isEmpty()) {
            continue;
          }
          // Over-cap: omit this dim from the stamped filter map (still a DimensionValueSetShardSpec for
          // class-uniformity; possibleInDomain treats an absent dim as unconstrained, so pruning is disabled
          // for it on this segment).
          if (maxValuesPerDimension != null && vals.size() > maxValuesPerDimension) {
            log.warn(
                "Segment[%s] dimension[%s] observed [%d] distinct values, exceeds maxValuesPerDimension[%d]; "
                + "pruning disabled for this dimension on this segment.",
                lookupKey,
                dim,
                vals.size(),
                maxValuesPerDimension
            );
            continue;
          }
          snapshot = new ArrayList<>(vals);
        }
        // Sort for deterministic published metadata; null (missing value) sorts first.
        snapshot.sort(Comparator.nullsFirst(Comparator.naturalOrder()));
        snapshotFilters.put(dim, snapshot);
      }
    }
    return s.withShardSpec(
        new DimensionValueSetShardSpec(
            s.getShardSpec().getPartitionNum(),
            s.getShardSpec().getNumCorePartitions(),
            snapshotFilters
        )
    );
  }

  @Override
  public void onSegmentPublished(SegmentId segmentId)
  {
    observedPartitionDimValuesBySegment.remove(segmentId);
    restartSpannedSegments.remove(segmentId);
  }
}
