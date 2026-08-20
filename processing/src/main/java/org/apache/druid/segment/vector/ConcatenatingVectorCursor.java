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

package org.apache.druid.segment.vector;

import com.google.common.base.Supplier;
import org.apache.druid.error.DruidException;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.projections.ClusteringVectorColumnSelectorFactory;

import java.util.List;
import java.util.Map;

/**
 * Vector-cursor counterpart of {@link org.apache.druid.segment.ConcatenatingCursor}. Walks a sequence of per-group
 * {@link VectorCursor}s back-to-back, presenting them to the caller as a single vector cursor over a clustered base
 * table. Vector boundaries do not align with cluster-group boundaries; the last vector of each group is naturally
 * partial (its {@link #getCurrentVectorSize()} returns less than max), and the next {@link #advance()} swaps in the
 * next group's vector cursor without trying to merge across groups.
 * <p/
 * >On each group transition the wrapper {@link ClusteringVectorColumnSelectorFactory} updates its underlying
 * delegate and clustering values, so previously-acquired delegating vector selectors observe the new group's data via
 * generation-counter cache invalidation.
 * <p/>
 * The outer {@link CursorHolder} owns the lifetime of the per-group holders (typically by registering each one
 * with a {@link org.apache.druid.java.util.common.io.Closer} as part of the supplier itself)
 */
public final class ConcatenatingVectorCursor implements VectorCursor
{
  private final List<Supplier<CursorHolder>> holderSuppliers;
  private final List<Object[]> clusteringValuesByGroup;
  private final ClusteringVectorColumnSelectorFactory wrapperFactory;
  private final VectorColumnSelectorFactory exposedFactory;

  private int currentIdx;
  private VectorCursor currentCursor;
  private boolean initialized;

  public ConcatenatingVectorCursor(
      List<Supplier<CursorHolder>> holderSuppliers,
      List<Object[]> clusteringValuesByGroup,
      ClusteringVectorColumnSelectorFactory wrapperFactory,
      Map<String, String> virtualColumnRemap
  )
  {
    if (holderSuppliers.size() != clusteringValuesByGroup.size()) {
      throw DruidException.defensive(
          "holderSuppliers size [%s] must equal clusteringValuesByGroup size [%s]",
          holderSuppliers.size(),
          clusteringValuesByGroup.size()
      );
    }
    if (holderSuppliers.isEmpty()) {
      throw DruidException.defensive("ConcatenatingVectorCursor requires at least one cluster group");
    }
    this.holderSuppliers = holderSuppliers;
    this.clusteringValuesByGroup = clusteringValuesByGroup;
    this.wrapperFactory = wrapperFactory;
    this.exposedFactory = virtualColumnRemap.isEmpty()
                          ? wrapperFactory
                          : new RemapVectorColumnSelectorFactory(wrapperFactory, virtualColumnRemap);
    this.currentIdx = -1;
  }

  private void initializeIfNeeded()
  {
    if (initialized) {
      return;
    }
    initialized = true;
    advanceToNextNonEmptyGroup();
  }

  /**
   * Open the next group whose vector cursor has at least one row. Skips empty groups. Sets {@code currentCursor =
   * null} when all groups are exhausted. The wrapper deliberately keeps the last group's delegate at exhaustion:
   * selector values are undefined after {@link #isDone()} anyway, but factory metadata (getColumnCapabilities)
   * must stay answerable.
   */
  private void advanceToNextNonEmptyGroup()
  {
    while (++currentIdx < holderSuppliers.size()) {
      final CursorHolder holder = holderSuppliers.get(currentIdx).get();
      final VectorCursor cursor = holder.asVectorCursor();
      if (cursor != null && !cursor.isDone()) {
        currentCursor = cursor;
        wrapperFactory.setDelegate(cursor.getColumnSelectorFactory(), clusteringValuesByGroup.get(currentIdx));
        return;
      }
      // Group has no rows after filter application; try the next.
    }
    currentCursor = null;
  }

  @Override
  public VectorColumnSelectorFactory getColumnSelectorFactory()
  {
    initializeIfNeeded();
    return exposedFactory;
  }

  @Override
  public void advance()
  {
    initializeIfNeeded();
    if (currentCursor == null) {
      return;
    }
    currentCursor.advance();
    if (currentCursor.isDone()) {
      advanceToNextNonEmptyGroup();
    }
  }

  @Override
  public boolean isDone()
  {
    initializeIfNeeded();
    return currentCursor == null;
  }

  /**
   * Rewind to before the first group. Does not close any per-group holders, those are owned by the outer
   * {@link CursorHolder}. Subsequent {@link #advance()} / {@link #isDone()} re-fetch each group's vector cursor via
   * {@link CursorHolder#asVectorCursor}.
   */
  @Override
  public void reset()
  {
    currentIdx = -1;
    currentCursor = null;
    initialized = false;
  }

  @Override
  public int getMaxVectorSize()
  {
    return wrapperFactory.getMaxVectorSize();
  }

  @Override
  public int getCurrentVectorSize()
  {
    initializeIfNeeded();
    if (currentCursor == null) {
      return 0;
    }
    return currentCursor.getCurrentVectorSize();
  }
}
