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

package org.apache.druid.segment;

import com.google.common.base.Supplier;
import it.unimi.dsi.fastutil.ints.IntHeapPriorityQueue;
import org.apache.druid.error.DruidException;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.projections.MergingColumnSelectorFactory;

import java.util.List;
import java.util.Map;

/**
 * {@link Cursor} that presents a set of individually {@code __time}-sorted per-cluster-group cursors as a single
 * globally {@code __time}-ordered cursor, via a streaming k-way merge. Used for clustered base tables where
 * {@code __time} is the first non-clustering column (so each group, whose clustering prefix is constant, is sorted on
 * {@code __time}); built by {@code QueryableIndexCursorFactory#makeTimeMergedClusteredCursorHolder}.
 * <p>
 * This is the time-ordered sibling of {@link ConcatenatingCursor}: where the concatenating cursor walks groups
 * back-to-back (order = {@code [clustering…, __time, …]}), this cursor interleaves them by {@code __time}. It opens
 * <em>all</em> surviving group cursors up front (so unlike the concatenating path it does not benefit from early-exit
 * laziness) and, on each {@link #advance()}, emits the row with the smallest (or largest, when descending)
 * {@code __time} across the groups. Each per-group sub-index already exposes the clustering columns as constants, so
 * the {@link MergingColumnSelectorFactory} simply dispatches every column to the winning group. When the plan carries a
 * query-virtual-column-to-materialized-column remap, the merging factory is wrapped in a
 * {@link RemapColumnSelectorFactory} so reads of a remapped query virtual column's output name resolve to its
 * materialized column (mirroring {@link ConcatenatingCursor}); the per-group specs already dropped those virtual
 * columns and added their materialized targets as physical columns.
 * <p>
 * An {@link IntHeapPriorityQueue} of group indices, keyed on each group's current {@code __time} via
 * {@link #compareGroups}, drives the merge: the queue head is the winning group, {@link IntHeapPriorityQueue#changed()}
 * re-settles it after its cursor advances, and it is dropped once exhausted. Ties on {@code __time} across groups break
 * by group index (arbitrary but deterministic); the advertised ordering is only {@code [__time]}, so secondary sort
 * columns are not preserved across groups. The outer {@link CursorHolder} owns the lifecycle of the per-group holders.
 */
public final class MergingClusterGroupCursor implements Cursor
{
  private final List<Supplier<CursorHolder>> holderSuppliers;
  private final boolean descending;
  private final Map<String, String> virtualColumnRemap;

  private boolean initialized;
  // Indexed by group. groupCursors[i] is null if that group's holder produced no cursor.
  private Cursor[] groupCursors;
  private BaseLongColumnValueSelector[] timeSelectors;
  private long[] currentTimes;
  // Group indices ordered by current __time (min for ascending, max for descending); the head is the winning group.
  private IntHeapPriorityQueue heap;
  // heap head while not done; retains the last winner once done (selectors are undefined after isDone() anyway).
  private int currentGroup;
  // Monotonically increasing output-row id, one per emitted row; the merge emits exactly one row per advance.
  private long outputRowId;
  // The factory exposed to callers: the MergingColumnSelectorFactory, wrapped in a RemapColumnSelectorFactory when
  // virtualColumnRemap is non-empty.
  private ColumnSelectorFactory exposedFactory;

  public MergingClusterGroupCursor(
      List<Supplier<CursorHolder>> holderSuppliers,
      boolean descending,
      Map<String, String> virtualColumnRemap
  )
  {
    if (holderSuppliers.isEmpty()) {
      throw DruidException.defensive("MergingClusterGroupCursor requires at least one cluster group");
    }
    this.holderSuppliers = holderSuppliers;
    this.descending = descending;
    this.virtualColumnRemap = virtualColumnRemap;
  }

  private void initializeIfNeeded()
  {
    if (initialized) {
      return;
    }
    initialized = true;
    final int n = holderSuppliers.size();
    groupCursors = new Cursor[n];
    timeSelectors = new BaseLongColumnValueSelector[n];
    currentTimes = new long[n];
    heap = new IntHeapPriorityQueue(this::compareGroups);
    outputRowId = 0;
    final ColumnSelectorFactory[] groupFactories = new ColumnSelectorFactory[n];
    for (int i = 0; i < n; i++) {
      final CursorHolder holder = holderSuppliers.get(i).get();
      final Cursor cursor = holder.asCursor();
      groupCursors[i] = cursor;
      if (cursor == null) {
        continue;
      }
      // A per-group cursor is still queryable for its factory/capabilities even when empty (done); only non-empty
      // groups join the heap.
      groupFactories[i] = cursor.getColumnSelectorFactory();
      timeSelectors[i] = groupFactories[i].makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME);
      if (!cursor.isDone()) {
        currentTimes[i] = timeSelectors[i].getLong();
        heap.enqueue(i);
      }
    }
    currentGroup = heap.isEmpty() ? 0 : heap.firstInt();
    final MergingColumnSelectorFactory mergingFactory =
        new MergingColumnSelectorFactory(groupFactories, () -> currentGroup, () -> outputRowId);
    exposedFactory = virtualColumnRemap.isEmpty()
                     ? mergingFactory
                     : new RemapColumnSelectorFactory(mergingFactory, virtualColumnRemap);
  }

  @Override
  public ColumnSelectorFactory getColumnSelectorFactory()
  {
    initializeIfNeeded();
    return exposedFactory;
  }

  @Override
  public void advance()
  {
    if (isDone()) {
      return;
    }
    final int root = heap.firstInt();
    groupCursors[root].advance();
    settleRoot(root);
    outputRowId++;
  }

  @Override
  public void advanceUninterruptibly()
  {
    if (isDone()) {
      return;
    }
    final int root = heap.firstInt();
    groupCursors[root].advanceUninterruptibly();
    settleRoot(root);
    outputRowId++;
  }

  /**
   * Re-settle the heap after the winning group's cursor has been advanced: drop the group if it is now exhausted,
   * otherwise re-key it on its new {@code __time} and let the queue re-sift the head. Refreshes {@link #currentGroup}.
   */
  private void settleRoot(int root)
  {
    if (groupCursors[root].isDone()) {
      heap.dequeueInt();
    } else {
      currentTimes[root] = timeSelectors[root].getLong();
      heap.changed();
    }
    if (!heap.isEmpty()) {
      currentGroup = heap.firstInt();
    }
  }

  @Override
  public boolean isDone()
  {
    initializeIfNeeded();
    return heap.isEmpty();
  }

  @Override
  public boolean isDoneOrInterrupted()
  {
    return isDone() || Thread.currentThread().isInterrupted();
  }

  @Override
  public void reset()
  {
    if (!initialized) {
      return;
    }
    // Reset the held per-group cursors in place (do NOT re-fetch via asCursor(), which would produce fresh factories
    // and invalidate the selectors already handed out through the MergingColumnSelectorFactory), then rebuild the heap.
    heap.clear();
    outputRowId = 0;
    for (int i = 0; i < groupCursors.length; i++) {
      final Cursor cursor = groupCursors[i];
      if (cursor == null) {
        continue;
      }
      cursor.reset();
      if (!cursor.isDone()) {
        currentTimes[i] = timeSelectors[i].getLong();
        heap.enqueue(i);
      }
    }
    currentGroup = heap.isEmpty() ? 0 : heap.firstInt();
  }

  /**
   * Heap ordering over group indices: earlier {@code __time} sits at the head for ascending, later for descending,
   * breaking ties by group index for determinism.
   */
  private int compareGroups(int a, int b)
  {
    final int cmp = descending
                    ? Long.compare(currentTimes[b], currentTimes[a])
                    : Long.compare(currentTimes[a], currentTimes[b]);
    return cmp != 0 ? cmp : Integer.compare(a, b);
  }
}
