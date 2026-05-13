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
import org.apache.druid.error.DruidException;
import org.apache.druid.segment.projections.ClusteringColumnSelectorFactory;

import javax.annotation.Nullable;
import java.util.List;

/**
 * {@link Cursor} that walks a sequence of per-group cursors back-to-back, presenting them to the caller as a single
 * cursor over a clustered base table. On group transitions the wrapper {@link ColumnSelectorFactory} updates its
 * underlying delegate and clustering values so previously-acquired delegating selectors observe the new group's data
 * on their next access.
 * <p>
 * Each entry in {@link #holderSuppliers} is a lazy producer of a {@link CursorHolder} for one cluster group. The
 * outer {@link CursorHolder} owns the lifecycle of the per-group holders.
 */
public final class ConcatenatingCursor implements Cursor
{
  private final List<Supplier<CursorHolder>> holderSuppliers;
  private final List<Object[]> clusteringValuesByGroup;
  private final ClusteringColumnSelectorFactory wrapperFactory;

  private int currentIdx;
  @Nullable
  private Cursor currentCursor;
  private boolean initialized;

  public ConcatenatingCursor(
      List<Supplier<CursorHolder>> holderSuppliers,
      List<Object[]> clusteringValuesByGroup,
      ClusteringColumnSelectorFactory wrapperFactory
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
      throw DruidException.defensive("ConcatenatingCursor requires at least one cluster group");
    }
    this.holderSuppliers = holderSuppliers;
    this.clusteringValuesByGroup = clusteringValuesByGroup;
    this.wrapperFactory = wrapperFactory;
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
   * Open the next group whose cursor has at least one row. Sets {@code currentCursor = null} when all groups are
   * exhausted.
   */
  private void advanceToNextNonEmptyGroup()
  {
    while (++currentIdx < holderSuppliers.size()) {
      final CursorHolder holder = holderSuppliers.get(currentIdx).get();
      final Cursor cursor = holder.asCursor();
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
  public ColumnSelectorFactory getColumnSelectorFactory()
  {
    initializeIfNeeded();
    return wrapperFactory;
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
  public void advanceUninterruptibly()
  {
    initializeIfNeeded();
    if (currentCursor == null) {
      return;
    }
    currentCursor.advanceUninterruptibly();
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

  @Override
  public boolean isDoneOrInterrupted()
  {
    return isDone() || Thread.currentThread().isInterrupted();
  }

  @Override
  public void reset()
  {
    currentIdx = -1;
    currentCursor = null;
    initialized = false;
  }
}
