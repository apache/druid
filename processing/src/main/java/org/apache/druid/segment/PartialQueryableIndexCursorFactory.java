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

import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.common.asyncresource.AsyncResource;
import org.apache.druid.common.asyncresource.AsyncResources;
import org.apache.druid.common.asyncresource.SettableAsyncResource;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.Order;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.file.PartialSegmentFileMapperV10;
import org.apache.druid.segment.vector.VectorCursor;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.function.Supplier;

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
 * <b>Async download granularity.</b> {@link PartialQueryableIndex#planCursorPrefetch} resolves what the spec needs.
 * When the segment's metadata records per-column file lists
 * ({@link org.apache.druid.segment.file.SegmentFileMetadata#getColumnFiles()}), the plan covers the required columns'
 * complete internal-file sets up front (including serde sub-files such as nested-column field files) as coalesced
 * range reads; for older segments without file lists it degrades to whole-bundle granularity, losing column-level
 * pruning within the bundle but keeping the matched-bundle pruning and covering serde sub-files that can't be
 * enumerated without the lists. Either way the plan's runs are coalesced up to the configured gap tolerance and split
 * at the configured run-size cap (see {@link PartialSegmentFileMapperV10#planParallelFetch}), and the cursor holder
 * constructed afterward sees the already-materialized holders via the same memoized suppliers, so no further
 * downloads happen at cursor-read time.
 * <p>
 * If a projection matches, the required columns are looked up against the projection's row selector and its rewritten
 * {@link CursorBuildSpec} (which carries physical columns in the projection's namespace). When
 * {@link CursorBuildSpec#getPhysicalColumns()} is {@code null}, every column on the chosen row selector is pre-fetched
 * as required by the contract of {@link CursorBuildSpec}.
 * <p>
 * <b>Parallelism.</b> Every planned range read is its own task on the download executor, so a bundle's independent
 * deep-storage requests proceed concurrently rather than paying their round trips sequentially. Once every run is
 * resident, the ready callback materializes each bundle's required columns and the cursor holder is constructed.
 * <p>
 * <b>Restricted serving.</b> When constructed with a {@code servableBundles} predicate (see
 * {@link org.apache.druid.segment.loading.AcquireMode#PARTIAL_RULE_HELD}), both paths plan only over the bundles it
 * accepts, and a spec that nothing servable can answer gets an {@link EmptyCursorHolder}. Accepted bundles that are not
 * resident are still downloaded by the async path as usual.
 * <p>
 * <b>Prefetch.</b> {@link #prefetch} shares the async path's machinery but, instead of a cursor holder, produces a
 * {@link CursorPrefetch} that owns the bundle holds. A later {@link #makeCursorHolderAsync} for the same spec (made
 * while the prefetch is still open) plans no downloads as everything is resident.
 */
public class PartialQueryableIndexCursorFactory implements CursorFactory
{
  private final PartialQueryableIndex index;
  private final QueryableIndexCursorFactory delegate;
  private final PartialBundleAcquirer bundleAcquirer;
  // when non-null, only the bundles this accepts are served, as if they were the whole segment; see
  // PartialQueryableIndex#planCursorPrefetch(CursorBuildSpec, Predicate)
  @Nullable
  private final Predicate<String> servableBundles;

  public PartialQueryableIndexCursorFactory(
      PartialQueryableIndex index,
      TimeBoundaryInspector timeBoundaryInspector,
      PartialBundleAcquirer bundleAcquirer
  )
  {
    this(index, timeBoundaryInspector, bundleAcquirer, null);
  }

  public PartialQueryableIndexCursorFactory(
      PartialQueryableIndex index,
      TimeBoundaryInspector timeBoundaryInspector,
      PartialBundleAcquirer bundleAcquirer,
      @Nullable Predicate<String> servableBundles
  )
  {
    this.index = index;
    this.delegate = new QueryableIndexCursorFactory(index, timeBoundaryInspector);
    this.bundleAcquirer = bundleAcquirer;
    this.servableBundles = servableBundles;
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
    if (servableBundles == null) {
      return delegate.makeCursorHolder(spec);
    }
    // restricted: dispatch from the restricted plan rather than letting the delegate match over the whole segment
    final PartialQueryableIndex.CursorPrefetchPlan plan = index.planCursorPrefetch(spec, servableBundles);
    if (plan.bundles().isEmpty()) {
      return EmptyCursorHolder.forSpec(spec, delegate);
    }
    if (plan.clusterGroupPlan() != null) {
      return delegate.makeClusteredCursorHolder(spec, plan.clusterGroupPlan());
    }
    return delegate.makeCursorHolderForProjection(spec, plan.matchedProjection());
  }

  @Override
  public AsyncCursorHolder makeCursorHolderAsync(CursorBuildSpec spec)
  {
    // The index resolves what the spec needs (projection match / surviving cluster groups / base table, required
    // columns, planned range reads); this factory purely orchestrates the downloads and cursor wiring.
    final PartialQueryableIndex.CursorPrefetchPlan plan = index.planCursorPrefetch(spec, servableBundles);
    if (plan.bundles().isEmpty()) {
      // Filter rules out every cluster group, or nothing servable can answer the spec: no bundle to acquire, nothing
      // to download.
      return AsyncCursorHolder.completed(EmptyCursorHolder.forSpec(spec, delegate));
    }
    if (plan.clusterGroupPlan() != null) {
      // reuse the plan's cluster resolution
      return buildAsyncCursorHolder(
          plan.bundles(),
          () -> delegate.makeClusteredCursorHolder(spec, plan.clusterGroupPlan())
      );
    }
    return buildAsyncCursorHolder(
        plan.bundles(),
        () -> delegate.makeCursorHolderForProjection(spec, plan.matchedProjection())
    );
  }

  @Override
  public CursorPrefetch prefetch(CursorBuildSpec spec)
  {
    final PartialQueryableIndex.CursorPrefetchPlan plan = index.planCursorPrefetch(spec, servableBundles);
    if (plan.bundles().isEmpty()) {
      return CursorPrefetch.none();
    }
    final PendingDownloads pending = acquireHoldsAndSubmitRuns(plan.bundles());
    final CursorPrefetch prefetch = new CursorPrefetch(pending.canceler());
    completeOnDownloadExecutor(
        pending,
        prefetch,
        () -> {
          materializeRequiredColumns(plan.bundles());
          final Closeable holds = releasers(pending.holdReleases());
          return new ResourceHolder<>()
          {
            @Override
            public CursorPrefetch.Stats get()
            {
              return pending.stats();
            }

            @Override
            public void close()
            {
              CloseableUtils.closeAndWrapExceptions(holds);
            }
          };
        }
    );
    return prefetch;
  }

  /**
   * Builds an async-holder of one or more bundles using {@link #acquireHoldsAndSubmitRuns} on the calling
   * thread, then {@link #completeOnDownloadExecutor} with a continuation that materializes each bundle's required
   * columns and builds the cursor holder. The produced holder takes ownership of all closeables.
   */
  private AsyncCursorHolder buildAsyncCursorHolder(
      List<PartialQueryableIndex.PrefetchBundle> bundles,
      Supplier<CursorHolder> innerBuilder
  )
  {
    final PendingDownloads pending = acquireHoldsAndSubmitRuns(bundles);
    final AsyncCursorHolder asyncHolder = new AsyncCursorHolder(pending.canceler());
    completeOnDownloadExecutor(
        pending,
        asyncHolder,
        () -> {
          materializeRequiredColumns(bundles);
          final CursorHolder inner = innerBuilder.get();
          return ResourceHolder.fromCloseable(wrapWithBundleRelease(inner, releasers(pending.holdReleases())));
        }
    );
    return asyncHolder;
  }

  /**
   * The calling-thread half of a cursor build or prefetch. Each {@link PartialQueryableIndex.PrefetchBundle} mounts
   * its cache-layer bundle (its own {@link BundleHoldRelease}, since eviction is per-bundle) and submits one download
   * task per planned range read under that hold. Returns the holds and the collected downloads.
   */
  private PendingDownloads acquireHoldsAndSubmitRuns(List<PartialQueryableIndex.PrefetchBundle> bundles)
  {
    final List<BundleHoldRelease> holdReleases = new ArrayList<>(bundles.size());
    try {
      final List<AsyncResource<String>> runDownloads = new ArrayList<>();
      final List<String> bundleNames = new ArrayList<>(bundles.size());
      long plannedBytes = 0;
      for (PartialQueryableIndex.PrefetchBundle bundle : bundles) {
        final BundleHoldRelease holdRelease = new BundleHoldRelease(bundleAcquirer.acquire(bundle.bundleName()));
        holdReleases.add(holdRelease);
        bundleNames.add(bundle.bundleName());
        // Plan the bundle's range reads only now, under its freshly-acquired hold
        for (PartialSegmentFileMapperV10.PlannedFetch fetch : bundle.planFetches()) {
          plannedBytes += fetch.run().length();
          runDownloads.add(submitRunFetch(bundle.bundleName(), fetch, holdRelease));
        }
      }
      return new PendingDownloads(
          holdReleases,
          AsyncResources.collect(runDownloads),
          new CursorPrefetch.Stats(List.copyOf(bundleNames), runDownloads.size(), plannedBytes)
      );
    }
    catch (Throwable t) {
      // Failure while acquiring holds or submitting downloads (e.g. a bundle reservation finding no room, or a
      // submitDownload shutdown rejection). A run download submitted before the failure may already be in flight, so
      // release every bundle hold through the handshake (requestRelease defers to the last in-flight body) rather
      // than dropping it directly mid-mapFile().
      throw CloseableUtils.closeAndWrapInCatch(t, () -> holdReleases.forEach(BundleHoldRelease::requestRelease));
    }
  }

  /**
   * The asynchronous half of a cursor build or prefetch, once every run download is ready, runs
   * {@code produceUnderHolds} as its own download-executor task inside every bundle's hold handshake (see
   * {@link #produce}) and hand what it produced to {@code target}, which takes ownership of it (closing it releases
   * every bundle hold, so the producer must fold {@link #releasers} into it).
   */
  private <T> void completeOnDownloadExecutor(
      PendingDownloads pending,
      SettableAsyncResource<T> target,
      Supplier<ResourceHolder<T>> produceUnderHolds
  )
  {
    // This callback can run on ANY thread: the download-executor thread that completed the last run, or, whenever
    // every run finished before registration, synchronously on THIS thread, because addReadyCallback on an
    // already-ready resource fires inline. So do no real work here: propagate failure/cancellation inline, and hop
    // the success path to the download executor.
    pending.downloaded().addReadyCallback(() -> {
      try {
        // ready, so never blocks; throws the run-download failure (or cancellation) if any. Failing here rather
        // than in the continuation keeps a canceled build from queueing a pointless continuation task.
        pending.downloaded().get();
      }
      catch (Throwable t) {
        fail(pending.holdReleases(), target, t);
        return;
      }
      try {
        bundleAcquirer.submitDownload(() -> {
          produce(pending, target, produceUnderHolds);
          return "produce";
        });
      }
      catch (Throwable t) {
        // the continuation couldn't even be submitted (e.g. executor shutdown rejection)
        fail(pending.holdReleases(), target, t);
      }
    });
  }

  /**
   * Continuation of {@link #completeOnDownloadExecutor} once every run download is ready, always running as its own
   * task on the download executor (never on the thread that observed readiness). The hand-off happens only after the
   * handshakes are closed again.
   */
  private static <T> void produce(
      PendingDownloads pending,
      SettableAsyncResource<T> target,
      Supplier<ResourceHolder<T>> produceUnderHolds
  )
  {
    final ResourceHolder<T> produced;
    try {
      // the ready callback only submits this continuation on success, but the consumer may close the target in the
      // window before this task runs; re-checking surfaces such a cancellation
      pending.downloaded().get();
      produced = runUnderHolds(pending.holdReleases(), 0, produceUnderHolds);
      if (produced == null) {
        // a concurrent cancel requested a hold release before we could open the handshake on every bundle; the
        // consumer is gone, so release the rest of the holds and surface the cancellation
        fail(
            pending.holdReleases(),
            target,
            DruidException.defensive("Async resource was closed before it could be produced")
        );
        return;
      }
    }
    catch (Throwable t) {
      fail(pending.holdReleases(), target, t);
      return;
    }
    if (!target.set(produced)) {
      // the target was closed (consumer canceled) while we were producing; close what we produced ourselves so it,
      // and the bundle holds it owns, don't leak.
      produced.close();
    }
  }

  /**
   * Shared failure path: request every bundle hold's release and fail the target. Releases go through the
   * {@link BundleHoldRelease} handshake (never dropped directly) because failure can be observed while a sibling
   * download is still mid-{@code mapFile()}.
   */
  private static void fail(List<BundleHoldRelease> holdReleases, SettableAsyncResource<?> target, Throwable t)
  {
    holdReleases.forEach(BundleHoldRelease::requestRelease);
    target.setException(t);
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
   * Submit one planned range read as its own download task, running under the bundle-hold handshake (see
   * {@link BundleHoldRelease#runDownloadBody}) so the bundle can't be evicted while the run is streaming into its
   * container (bundle cache entries own their external-resident containers too, so the hold covers runs on external
   * mappers just the same). The returned token (the bundle name) is unused; the task's effect is making the run's
   * files resident in the mapper that planned it.
   */
  private AsyncResource<String> submitRunFetch(
      String bundleName,
      PartialSegmentFileMapperV10.PlannedFetch fetch,
      BundleHoldRelease holdRelease
  )
  {
    return bundleAcquirer.submitDownload(() -> {
      holdRelease.runDownloadBody(() -> {
        try {
          fetch.fetch();
        }
        catch (IOException e) {
          throw DruidException.defensive(e, "Failed to fetch files for bundle[%s]", bundleName);
        }
      });
      return bundleName;
    });
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
   * Compose every bundle's success-path {@link BundleHoldRelease#releaser} into one {@link Closeable} that the produced
   * cursor holder owns: closing the holder releases all the bundle holds together.
   */
  private static Closeable releasers(List<BundleHoldRelease> holdReleases)
  {
    final Closer closer = Closer.create();
    holdReleases.forEach(holdRelease -> closer.register(holdRelease.releaser()));
    return closer;
  }

  /**
   * Runs {@code body} with every bundle's handshake open, by nesting one {@link BundleHoldRelease#runDownloadBody}
   * per hold. Returns {@code null} without running {@code body} if any hold has already been asked to release
   * (concurrent cancel), since its mmaps may be about to go away.
   */
  @Nullable
  private static <T> T runUnderHolds(List<BundleHoldRelease> holdReleases, int nextHold, Supplier<T> body)
  {
    if (nextHold < holdReleases.size()) {
      return holdReleases.get(nextHold).runDownloadBody(() -> runUnderHolds(holdReleases, nextHold + 1, body));
    }
    return body.get();
  }

  /**
   * Materialize (deserialize and memoize) each bundle's required columns. Touches the container mmaps, so call only
   * with every bundle's handshake open (see {@link #runUnderHolds}).
   */
  private static void materializeRequiredColumns(List<PartialQueryableIndex.PrefetchBundle> bundles)
  {
    for (PartialQueryableIndex.PrefetchBundle bundle : bundles) {
      for (String column : bundle.requiredColumns()) {
        bundle.rowSelector().getColumnHolder(column);
      }
    }
  }

  /**
   * The calling-thread result of {@link #acquireHoldsAndSubmitRuns}: every bundle's hold, the collected run
   * downloads submitted under them, and what was planned (for {@link CursorPrefetch.Stats}).
   */
  private record PendingDownloads(
      List<BundleHoldRelease> holdReleases,
      AsyncResource<List<String>> downloaded,
      CursorPrefetch.Stats stats
  )
  {
    /**
     * Canceler for the resource being produced, run if its consumer closes it before it's ready (e.g. query
     * cancel/timeout). Closes the collected resource to cancel every run download that hasn't begun its deep-storage
     * read yet (queued tasks are skipped; tasks parked on the download executor's permit are interrupted out of the
     * interruptible wait before doing any I/O), then requests the hold release on every bundle through the handshake.
     * Once the result is produced and handed to set(), ownership transfers to the consumer, which drains it via
     * close() (cancel) or release() (success); each bundle's once-guard keeps its hold release safe across all of
     * these paths.
     */
    Runnable canceler()
    {
      return () -> {
        CloseableUtils.closeAndSuppressExceptions(downloaded, ignored -> {});
        holdReleases.forEach(BundleHoldRelease::requestRelease);
      };
    }
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
     * Run one mmap-touching body under the in-flight count. {@code incrementAndGet}-before-read pairs with
     * {@code releaseRequested.set(true)}-before-read in {@link #requestRelease()}: whichever thread observes the
     * other's write takes responsibility for the hold release, so it happens exactly once and never while a
     * {@code mapFile()} is in flight. A body that observes a release request before starting is skipped entirely,
     * returning {@code null} (its containers are about to be released and may be evicted), so the body itself must
     * never return {@code null}; the last body to finish after a release request releases the hold.
     */
    @Nullable
    <T> T runDownloadBody(Supplier<T> read)
    {
      bodiesInFlight.incrementAndGet();
      try {
        if (!releaseRequested.get()) {
          return read.get();
        }
        return null;
      }
      finally {
        if (bodiesInFlight.decrementAndGet() == 0 && releaseRequested.get()) {
          CloseableUtils.closeAndSuppressExceptions(releaseHoldOnce, ignored -> {});
        }
      }
    }

    /**
     * {@link #runDownloadBody(Supplier)} for bodies with no result; the caller can't distinguish ran-from-skipped.
     */
    void runDownloadBody(Runnable read)
    {
      runDownloadBody(() -> {
        read.run();
        return Boolean.TRUE;
      });
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
     * The raw, idempotent bundle-hold close. Handed to the produced cursor holder on the success path so that closing
     * the holder releases the hold; it bypasses the in-flight handshake, which is safe there because the holder is only
     * built after every download has completed (nothing is mid-{@code mapFile()}). Cancel/failure paths must release
     * through {@link #requestRelease()} instead; the once-guard keeps the hold release exactly-once across whichever
     * path fires.
     */
    Closeable releaser()
    {
      return releaseHoldOnce;
    }
  }
}
