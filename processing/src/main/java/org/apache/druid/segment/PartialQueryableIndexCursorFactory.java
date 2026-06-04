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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.Order;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.projections.Projections;
import org.apache.druid.segment.projections.QueryableProjection;
import org.apache.druid.segment.vector.VectorCursor;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Partial-aware {@link CursorFactory} for {@link PartialQueryableIndex}.
 * <p>
 * <b>Sync vs async contract.</b> {@link #makeCursorHolder} requires the segment to already be fully downloaded,
 * intended for callers that acquired the segment via the eager (download-everything-up-front) path, so by the time
 * they ask for a cursor every internal file is on disk. If anything is missing it throws
 * {@link DruidException#defensive} so that we never trigger downloads on the sync path, since processing threads must
 * not block on deep-storage I/O. {@link #makeCursorHolderAsync} is the only path that performs downloads on demand;
 * callers acknowledge that by opting into the async variant when they acquire a partial segment.
 * <p>
 * <b>Async download granularity.</b> Pre-fetch is column-level. {@link #makeCursorHolderAsync} calls
 * {@link QueryableIndex#getColumnHolder} on each required column; the memoized supplier on the underlying
 * {@link PartialQueryableIndex} eagerly invokes
 * {@link org.apache.druid.segment.file.PartialSegmentFileMapperV10#mapFile} inside that call, which is what triggers
 * the deep-storage range read. The cursor holder constructed afterward sees the already-materialized holders via the
 * same memoized suppliers, so no further downloads happen at cursor-read time.
 * <p>
 * If a projection matches, the required columns are looked up against the projection's row selector and its rewritten
 * {@link CursorBuildSpec} (which carries physical columns in the projection's namespace). When
 * {@link CursorBuildSpec#getPhysicalColumns()} is {@code null}, every column on the chosen row selector is pre-fetched
 * as required by the contract of {@link CursorBuildSpec}.
 * <p>
 * <b>Parallelism.</b> Each column's materialization is submitted as a separate task to the supplied download executor.
 * The cursor holder is constructed once every column task has completed.
 */
public class PartialQueryableIndexCursorFactory implements CursorFactory
{
  private final PartialQueryableIndex index;
  private final QueryableIndexCursorFactory delegate;
  private final PartialBundleAcquirer bundleAcquirer;

  public PartialQueryableIndexCursorFactory(
      PartialQueryableIndex index,
      TimeBoundaryInspector timeBoundaryInspector,
      PartialBundleAcquirer bundleAcquirer
  )
  {
    this.index = index;
    this.delegate = new QueryableIndexCursorFactory(index, timeBoundaryInspector);
    this.bundleAcquirer = bundleAcquirer;
  }

  @Override
  public CursorHolder makeCursorHolder(CursorBuildSpec spec)
  {
    // refuse to download here so we never accidentally block a processing thread
    if (!index.isFullyDownloaded()) {
      throw DruidException.defensive(
          "Sync makeCursorHolder requires the segment to be fully downloaded; use makeCursorHolderAsync for "
          + "on-demand loading, or acquire the segment via the eager path so all files are loaded up front."
      );
    }
    return delegate.makeCursorHolder(spec);
  }

  @Override
  public AsyncCursorHolder makeCursorHolderAsync(CursorBuildSpec spec)
  {
    final QueryableProjection<QueryableIndex> matched = index.getProjection(spec);
    final QueryableIndex rowSelector = matched != null ? matched.getRowSelector() : index;
    final Set<String> requiredColumns = requiredColumns(rowSelector, matched, spec);
    final String bundleName = matched != null ? matched.getName() : Projections.BASE_TABLE_PROJECTION_NAME;

    // Mount the cache-layer bundle BEFORE submitting downloads. This sparse-allocates the bundle's container files
    // and reserves the disk-usage accounting at bundle granularity. The returned Closeable releases the hold when
    // the cursor closes, allowing the cache to later evict the bundle if needed.
    // PartialBundleAcquirer.acquire() throws DruidException on failure.
    final Closeable bundleHold = bundleAcquirer.acquire(bundleName);
    // Release the bundle hold at most once: the canceler, the success-path holder close, and the failure callback can
    // all race to release it.
    final AtomicBoolean holdReleased = new AtomicBoolean(false);
    final Closeable releaseHoldOnce = () -> {
      if (holdReleased.compareAndSet(false, true)) {
        bundleHold.close();
      }
    };

    try {
      // Submit one materialization task per column so a multi-threaded download executor can fan them out
      final ListeningExecutorService downloadExec = bundleAcquirer.getDownloadExec();
      final List<ListenableFuture<?>> columnDownloads = new ArrayList<>(requiredColumns.size());
      for (String column : requiredColumns) {
        columnDownloads.add(downloadExec.submit(() -> {
          rowSelector.getColumnHolder(column);
          return null;
        }));
      }

      // Canceler runs if the awaiter closes this holder before it's ready (e.g. query cancel/timeout). cancel(true)
      // stops column downloads that haven't begun their deep-storage read yet: queued tasks are skipped, and tasks
      // parked on the download executor's permit are interrupted out of the (interruptible) wait before doing any I/O.
      // A download already in its read/write loop runs to completion. After canceling, release the bundle hold. Once
      // the holder is produced and handed to set(), ownership transfers to the awaiter, which drains it via
      // close() (cancel) or release() (success); the once-guard keeps the hold release safe across all of these paths.
      final AsyncCursorHolder asyncHolder = new AsyncCursorHolder(() -> {
        for (ListenableFuture<?> columnDownload : columnDownloads) {
          columnDownload.cancel(true);
        }
        CloseableUtils.closeAndSuppressExceptions(releaseHoldOnce, ignored -> {});
      });
      Futures.addCallback(
          Futures.allAsList(columnDownloads),
          new FutureCallback<>()
          {
            @Override
            public void onSuccess(List<Object> ignored)
            {
              final CursorHolder holder;
              try {
                final CursorHolder inner = delegate.makeCursorHolderForProjection(spec, matched);
                // wrapWithBundleRelease takes ownership of both inner and the bundle hold; from here, closing holder
                // releases both. Only the makeCursorHolderForProjection build above can throw (the wrap cannot), so
                // the catch only needs to release the still-unowned bundle hold — no inner holder can escape it.
                holder = wrapWithBundleRelease(inner, releaseHoldOnce);
              }
              catch (Throwable t) {
                CloseableUtils.closeAndSuppressExceptions(releaseHoldOnce, t::addSuppressed);
                asyncHolder.setException(t);
                return;
              }
              if (!asyncHolder.set(holder)) {
                // wrapper was closed (awaiter cancelled) while we were producing the holder; close it ourselves so
                // the holder, its inner, and the bundle hold don't leak.
                holder.close();
              }
            }

            @Override
            public void onFailure(Throwable t)
            {
              // Includes the cancellation case: the canceler's cancel(true) makes Futures.allAsList complete
              // exceptionally with a CancellationException. releaseHoldOnce is a no-op if the canceler already ran.
              CloseableUtils.closeAndSuppressExceptions(releaseHoldOnce, t::addSuppressed);
              asyncHolder.setException(t);
            }
          },
          // Run the cursor-build callback inline on whichever download thread finishes last. The build itself does no
          // I/O (columns are already materialized), so it doesn't need to round-trip through the download executor.
          MoreExecutors.directExecutor()
      );
      return asyncHolder;
    }
    catch (Throwable t) {
      // Failure between acquire and the addCallback wiring (getDownloadExec, downloadExec.submit shut-down rejection,
      // Futures.addCallback rejecting on a bad executor, etc.). Ownership of the bundle hold hasn't transferred to the
      // callback yet, so release it here. Already-submitted download tasks will complete with no callback wired,
      // their captured row-selector refs drop naturally.
      throw CloseableUtils.closeAndWrapInCatch(t, releaseHoldOnce);
    }
  }

  @Override
  public RowSignature getRowSignature()
  {
    return delegate.getRowSignature();
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    return delegate.getColumnCapabilities(column);
  }


  /**
   * Determine the set of physical column names to required from the chosen row selector given a {@link CursorBuildSpec}
   */
  private static Set<String> requiredColumns(
      QueryableIndex rowSelector,
      @Nullable QueryableProjection<QueryableIndex> matched,
      CursorBuildSpec originalSpec
  )
  {
    final CursorBuildSpec effective = matched != null ? matched.getCursorBuildSpec() : originalSpec;
    if (effective.getPhysicalColumns() != null) {
      final Set<String> required = new LinkedHashSet<>(effective.getPhysicalColumns());
      // physicalColumns enumerates the selected columns, but QueryableIndexCursorHolder also reads __time while
      // building the cursor, independent of physicalColumns: unconditionally for a time-ordered index (its
      // interval-checking offset reads timestamps), and via a synthesized __time range filter for a non-time-ordered
      // index whose data extends past the query interval. That read happens after the cursor holder is handed back,
      // so __time must be pre-fetched on the async path or it becomes a lazy deep-storage download on a processing
      // thread. Predicting exactly when the holder reads __time would mean replicating its internals (fragile, and it
      // reads __time in the common cases anyway), so always include it: __time is cheap, and it resolves to a
      // no-download constant column for projections that don't carry a real time column.
      required.add(ColumnHolder.TIME_COLUMN_NAME);
      return required;
    }
    // Conservative fallback when physicalColumns isn't declared, fetch every column on the chosen row selector
    // plus __time (which is special-cased and not enumerated by getColumnNames()).
    final Set<String> all = new LinkedHashSet<>(rowSelector.getColumnNames());
    all.add(ColumnHolder.TIME_COLUMN_NAME);
    return all;
  }

  /**
   * Wrap a {@link CursorHolder} so that closing it also closes {@code bundleHold}, releasing the cache-layer
   * reservation hold on the bundle's containers.
   */
  private static CursorHolder wrapWithBundleRelease(CursorHolder inner, Closeable bundleHold)
  {
    return new CursorHolder()
    {
      @Override
      public Cursor asCursor()
      {
        return inner.asCursor();
      }

      @Override
      public VectorCursor asVectorCursor()
      {
        return inner.asVectorCursor();
      }

      @Override
      public boolean canVectorize()
      {
        return inner.canVectorize();
      }

      @Override
      public boolean isPreAggregated()
      {
        return inner.isPreAggregated();
      }

      @Override
      @Nullable
      public List<AggregatorFactory> getAggregatorsForPreAggregated()
      {
        return inner.getAggregatorsForPreAggregated();
      }

      @Override
      public List<OrderBy> getOrdering()
      {
        return inner.getOrdering();
      }

      @Override
      public Order getTimeOrder()
      {
        return inner.getTimeOrder();
      }

      @Override
      public void close()
      {
        final Closer closer = Closer.create();
        closer.register(bundleHold);
        closer.register(inner);
        CloseableUtils.closeAndWrapExceptions(closer);
      }
    };
  }
}
