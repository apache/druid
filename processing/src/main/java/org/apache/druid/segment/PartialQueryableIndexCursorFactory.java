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

import org.apache.druid.common.asyncresource.AsyncResource;
import org.apache.druid.common.asyncresource.AsyncResources;
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
import java.util.concurrent.atomic.AtomicInteger;

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

    // Mount the cache-layer bundle before submitting downloads. Released on the cancel/failure paths (requestRelease)
    // or handed to the produced cursor holder on success (releaser), exactly once, and never while an in-flight column
    // download is mid-mapFile(). See BundleHoldRelease.
    final BundleHoldRelease holdRelease = new BundleHoldRelease(bundleAcquirer.acquire(bundleName));

    try {
      // submit one materialization task per column so a multi-threaded download executor can fan them out
      final List<AsyncResource<String>> columnDownloads = new ArrayList<>(requiredColumns.size());
      for (String column : requiredColumns) {
        columnDownloads.add(submitColumnDownload(rowSelector, column, holdRelease));
      }
      final AsyncResource<List<String>> downloaded = AsyncResources.collect(columnDownloads);

      // Canceler runs if the awaiter closes this holder before it's ready (e.g. query cancel/timeout). Close the
      // collected resource to cancel every column download that hasn't begun its deep-storage read yet (queued tasks
      // are skipped; tasks parked on the download executor's permit are interrupted out of the interruptible wait
      // before doing any I/O), then request the hold release through the handshake. Once the holder is produced and
      // handed to set(), ownership transfers to the awaiter, which drains it via close() (cancel) or release()
      // (success); the once-guard keeps the hold release safe across all of these paths.
      final AsyncCursorHolder asyncHolder = new AsyncCursorHolder(() -> {
        CloseableUtils.closeAndSuppressExceptions(downloaded, ignored -> {});
        holdRelease.requestRelease();
      });
      downloaded.addReadyCallback(() -> {
        final CursorHolder holder;
        try {
          downloaded.get(); // surfaces any column-download failure (or cancellation) as the cause
          final CursorHolder inner = delegate.makeCursorHolderForProjection(spec, matched);
          // The produced holder takes ownership of both inner and the bundle hold; from here, closing it releases both
          holder = wrapWithBundleRelease(inner, holdRelease.releaser());
        }
        catch (Throwable t) {
          // A column download failed or was canceled, this branch can fire while a sibling download is still
          // mid-mapFile(), so the hold release must go through the handshake rather than dropping it here directly.
          holdRelease.requestRelease();
          asyncHolder.setException(t);
          return;
        }
        if (!asyncHolder.set(holder)) {
          // wrapper was closed (awaiter canceled) while we were producing the holder; close it ourselves so the
          // holder, its inner, and the bundle hold don't leak.
          holder.close();
        }
      });
      return asyncHolder;
    }
    catch (Throwable t) {
      // Failure between acquire and wiring up the downloads (submitDownload shut-down rejection, etc.). Ownership of
      // the bundle hold hasn't transferred to the holder yet, so release it here.
      throw CloseableUtils.closeAndWrapInCatch(t, holdRelease.releaser());
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
   * Submit one column's materialization as a download task, running its body under the bundle-hold handshake (see
   * {@link BundleHoldRelease#runDownloadBody}) so the hold stays alive until this body's
   * {@link QueryableIndex#getColumnHolder} (and the
   * {@link org.apache.druid.segment.file.PartialSegmentFileMapperV10#mapFile} it triggers) has finished. The returned
   * token (the column name) is unused, the task's effect is the side effect of materializing the column into the file
   * mapper.
   */
  private AsyncResource<String> submitColumnDownload(
      QueryableIndex rowSelector,
      String column,
      BundleHoldRelease holdRelease
  )
  {
    return bundleAcquirer.submitDownload(() -> {
      holdRelease.runDownloadBody(() -> rowSelector.getColumnHolder(column));
      return column;
    });
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

  /**
   * Owns a bundle hold's release for one {@link #makeCursorHolderAsync} build, guarding the file mapper's
   * evict-vs-mapFile contract: releasing the hold makes the bundle entry eligible for eviction, which unmaps the
   * bundle's containers, so a column download still inside {@code mapFile()} would then read (and slice a ByteBuffer
   * from) an unmapped region resulting in a JVM SIGSEGV. The hold is released exactly once (CAS-guarded) and only once
   * a release has been {@link #requestRelease() requested} (the awaiter canceled, or a download failed) AND no
   * download body is executing, so an in-flight {@code mapFile()} always finishes under the hold. On the success path
   * the produced cursor holder takes ownership of the hold via {@link #releaser()} and the request/in-flight handshake
   * is unused.
   */
  private static final class BundleHoldRelease
  {
    private final Closeable releaseHoldOnce;
    private final AtomicInteger bodiesInFlight = new AtomicInteger(0);
    private final AtomicBoolean releaseRequested = new AtomicBoolean(false);

    BundleHoldRelease(Closeable bundleHold)
    {
      // Release the bundle hold at most once: the canceler, the success-path holder close, the failure callback, and
      // the last download body can all race to release it.
      final AtomicBoolean released = new AtomicBoolean(false);
      this.releaseHoldOnce = () -> {
        if (released.compareAndSet(false, true)) {
          bundleHold.close();
        }
      };
    }

    /**
     * Run one column-materialization body under the in-flight count. {@code incrementAndGet}-before-read pairs with
     * {@code releaseRequested.set(true)}-before-read in {@link #requestRelease()}: whichever thread observes the
     * other's write takes responsibility for the hold release, so it happens exactly once and never while a
     * {@code mapFile()} is in flight. A body that observes a release request before starting skips the read entirely
     * (its containers are about to be released and may be evicted); the last body to finish after a release request
     * releases the hold.
     */
    void runDownloadBody(Runnable read)
    {
      bodiesInFlight.incrementAndGet();
      try {
        if (!releaseRequested.get()) {
          read.run();
        }
      }
      finally {
        if (bodiesInFlight.decrementAndGet() == 0 && releaseRequested.get()) {
          CloseableUtils.closeAndSuppressExceptions(releaseHoldOnce, ignored -> {});
        }
      }
    }

    /**
     * Request the hold release because the awaiter cancelled or a download failed. Releases now only if no download
     * body is executing; otherwise the last body out (see {@link #runDownloadBody}) releases it once its
     * {@code mapFile()} has finished. Idempotent.
     */
    void requestRelease()
    {
      releaseRequested.set(true);
      if (bodiesInFlight.get() == 0) {
        CloseableUtils.closeAndSuppressExceptions(releaseHoldOnce, ignored -> {});
      }
    }

    /**
     * The success-path releaser: ownership transfers to the produced cursor holder, whose close releases the bundle
     * hold. The once-guard keeps it safe against the cancel/failure paths.
     */
    Closeable releaser()
    {
      return releaseHoldOnce;
    }
  }
}
