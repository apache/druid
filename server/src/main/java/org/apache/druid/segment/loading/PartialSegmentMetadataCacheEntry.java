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

package org.apache.druid.segment.loading;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.common.asyncresource.AsyncResource;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.PartialBundleAcquirer;
import org.apache.druid.segment.PartialQueryableIndex;
import org.apache.druid.segment.PartialQueryableIndexSegment;
import org.apache.druid.segment.ReferenceCountingCloseableObject;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.file.PartialSegmentDownloadListener;
import org.apache.druid.segment.file.PartialSegmentFileMapperV10;
import org.apache.druid.segment.file.SegmentFileBuilder;
import org.apache.druid.segment.file.SegmentFileMetadata;
import org.apache.druid.segment.projections.Projections;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Cache entry for the metadata header of a V10 segment loaded via partial download. Mounting this entry range-reads
 * the V10 header from deep storage, parses {@link SegmentFileMetadata}, and constructs a
 * {@link PartialSegmentFileMapperV10} that can later download individual internal files on demand.
 * <p>
 * Reservation is sized via a configurable up-front estimate at construction time, then shrunk to the actual on-disk
 * header size after mount via {@link StorageLocation#adjustReservation}. Mount fails fast if the actual size exceeds
 * the estimate; the operator must increase the knob to recover.
 * <p>
 * Per-bundle cache entries created downstream of this one share the same {@link PartialSegmentFileMapperV10}
 * instance via {@link #getFileMapper()}; closing the metadata entry closes the file mapper, which unmaps all
 * containers and external file mappers.
 * <p>
 * <b>Reference-counted deferred cleanup.</b> {@link #unmount()} does not necessarily release resources synchronously.
 * Callers that need the file mapper to stay alive across an intervening drop (e.g. a query reading column data
 * through {@link PartialSegmentBundleCacheEntry}, or another component that needs the parsed
 * {@link SegmentFileMetadata}) acquire a reference via {@link #acquireMetadataReference()}; while any references are
 * outstanding, the actual close-file-mapper work is deferred. When the last reference releases the cleanup fires on
 * that thread. Bundle entries hold one such reference per active mount, so the typical pattern is: mount metadata,
 * mount bundle (which acquires a reference on metadata), use the bundle, unmount bundle (releases its reference and
 * triggers metadata cleanup if it was the last reference and metadata's own unmount has been called). The same
 * instance can be re-mounted after a previous cleanup completes; a fresh internal Phaser is installed on the next
 * successful mount.
 * <p>
 * <b>Deferred cleanup hook.</b> Callers can attach a {@link Runnable} via {@link #setOnUnmount} that fires once after
 * the mapper is closed in {@link #doActualUnmount}. This is the right place to schedule work that should run only when
 * the entry is truly purged.
 */
public class PartialSegmentMetadataCacheEntry implements SegmentCacheEntry, ResizableCacheEntry
{
  private static final EmittingLogger LOG = new EmittingLogger(PartialSegmentMetadataCacheEntry.class);

  private final SegmentCacheEntryIdentifier id;
  private final SegmentId segmentId;
  private final File localCacheDir;
  private final String targetFilename;
  private final List<String> externalFilenames;
  private final SegmentRangeReader rangeReader;
  private final ObjectMapper jsonMapper;
  private final long reservationEstimate;

  @Nullable
  private final StorageLoadingThreadPool storagePool;

  // ReentrantLock instead of synchronized to avoid pinning virtual threads pre-JEP 491
  private final ReentrantLock entryLock = new ReentrantLock();

  // current size for accounting; starts at the estimate, shrunk to actual on-disk size after mount
  @GuardedBy("entryLock")
  private long currentSize;

  // null until mounted
  @GuardedBy("entryLock")
  @Nullable
  private StorageLocation location;
  @GuardedBy("entryLock")
  @Nullable
  private PartialSegmentFileMapperV10 fileMapper;
  // Cached PartialQueryableIndex for the mounted file mapper. Built lazily on first {@code acquireReference} call
  // so concurrent acquireReference calls share the same memoized column-holder suppliers. first read of a column
  // deserializes once and the ColumnHolder stays cached for subsequent queries against the same entry. Cleared
  // during doActualUnmount so a subsequent mount rebuilds it against the fresh mapper.
  @GuardedBy("entryLock")
  @Nullable
  private PartialQueryableIndex queryableIndex;

  // Bundle-acquirer view of this entry, cached at construction. Stateless (delegates to the entry's mutable fields
  // under entryLock at call time), so safe to share across all acquireReference() calls.
  private final PartialBundleAcquirer bundleAcquirer;

  // Optional deferred-cleanup hook invoked by doActualUnmount after the mapper is closed.
  private final AtomicReference<Runnable> onUnmount = new AtomicReference<>();

  // bundle entries that are currently mounted against this segment, registered by PartialSegmentBundleCacheEntry on
  // successful mount and removed on unmount. Lets the drop path enumerate bundles for cascade-close without scanning
  // the StorageLocation's entry maps. Keyed by bundle name (one bundle per name per segment).
  private final ConcurrentHashMap<String, PartialSegmentBundleCacheEntry> linkedBundles = new ConcurrentHashMap<>();

  // Reference-counted gate over the actual cleanup work (close file mapper, delete header files). Set on
  // successful mount; unmount() closes the wrapper which defers running cleanup until all outstanding references
  // (acquired via acquireMetadataReference()) are released. Re-created on mount-after-cleanup-completion. Null when
  // the entry has never been mounted.
  private final AtomicReference<ReferenceCountingCloseableObject<Closeable>> references = new AtomicReference<>();

  // CAS+SettableFuture mount-dedup gate, mirroring the bundle entry's pattern. Without this, mount()'s slow range-read
  // would have to hold entryLock for its full duration, blocking concurrent status reads (isMounted, getSize, ...).
  // With it: one thread wins the CAS and runs doMount; the rest wait on the same future. On failure the gate is
  // cleared so retries get a fresh attempt; on success the gate stays set until doActualUnmount clears it.
  private final AtomicReference<SettableFuture<Void>> mountFuture = new AtomicReference<>();

  /**
   * Rule-holds state machine (all guarded by {@link #entryLock}). A partial-load rule is applied via
   * {@link #applyRule(String, Set)} after {@link #mount(StorageLocation)}. While a rule is applied
   * ({@code ruleFingerprint != null}):
   * <ul>
   *   <li>The entry holds a self-referential {@link StorageLocation.ReservationHold} on its own weak reservation,
   *       preventing cache from evicting the metadata entry.</li>
   *   <li>For each bundle name in {@link #ruleSelectedBundleNames} that has registered with this entry via
   *       {@link #registerBundle}, the entry holds a {@link StorageLocation.ReservationHold} on the bundle's weak
   *       reservation, preventing cache from evicting the bundle.</li>
   * </ul>
   * {@link #clearRule()} releases the holds and clears the state. {@link #applyRule(String, Set)} with a different
   * fingerprint / selection set diffs the current state and closes / acquires only the deltas, so overlapping bundle
   * holds stay live across a rule swap.
   */
  @GuardedBy("entryLock")
  @Nullable
  private String ruleFingerprint;
  @GuardedBy("entryLock")
  private Set<String> ruleSelectedBundleNames = Set.of();
  @GuardedBy("entryLock")
  private final Map<String, StorageLocation.ReservationHold<PartialSegmentBundleCacheEntry>> ruleBundleHolds =
      new HashMap<>();
  @GuardedBy("entryLock")
  @Nullable
  private StorageLocation.ReservationHold<PartialSegmentMetadataCacheEntry> metadataSelfHold;

  public PartialSegmentMetadataCacheEntry(
      SegmentId segmentId,
      File localCacheDir,
      String targetFilename,
      List<String> externalFilenames,
      SegmentRangeReader rangeReader,
      ObjectMapper jsonMapper,
      @Nullable StorageLoadingThreadPool storagePool,
      long reservationEstimate
  )
  {
    if (reservationEstimate <= 0) {
      throw DruidException.defensive(
          "Reservation estimate for partial metadata entry[%s] must be positive, got [%d]",
          segmentId,
          reservationEstimate
      );
    }
    this.segmentId = segmentId;
    this.id = new SegmentCacheEntryIdentifier(segmentId);
    this.localCacheDir = localCacheDir;
    this.targetFilename = targetFilename;
    this.externalFilenames = List.copyOf(externalFilenames);
    this.rangeReader = rangeReader;
    this.jsonMapper = jsonMapper;
    this.storagePool = storagePool;
    this.reservationEstimate = reservationEstimate;
    this.currentSize = reservationEstimate;
    this.bundleAcquirer = createBundleAcquirer();
  }

  @Override
  public SegmentCacheEntryIdentifier getId()
  {
    return id;
  }

  @Override
  public SegmentId getSegmentId()
  {
    return segmentId;
  }

  /**
   * The per-segment cache directory this entry reads from and writes to. Exposed for the bundle-restore helper in
   * {@link PartialSegmentCacheBootstrap#restoreBundlesFromDisk} so it can locate the on-disk container files.
   */
  File getLocalCacheDir()
  {
    return localCacheDir;
  }

  /**
   * The fingerprint of the partial-load rule currently applied to this entry, or {@code null} if no rule has been
   * applied. Set by {@link #applyRule(String, Set)} and cleared by {@link #clearRule()}. {@link #doActualUnmount()}
   * asserts that this field is {@code null} at unmount time, the {@code metadataSelfHold} taken by applyRule pins
   * the entry's phaser and prevents eviction reclaim / doActualUnmount from firing while a rule is applied. A fresh
   * mount therefore always starts with no rule applied.
   */
  @Nullable
  public String getRuleFingerprint()
  {
    entryLock.lock();
    try {
      return ruleFingerprint;
    }
    finally {
      entryLock.unlock();
    }
  }

  /**
   * The set of bundle names currently pinned by an applied partial-load rule, or an empty set if no rule is applied.
   * Return value is immutable; safe to inspect without further synchronization.
   */
  public Set<String> getRuleSelectedBundleNames()
  {
    entryLock.lock();
    try {
      return ruleSelectedBundleNames;
    }
    finally {
      entryLock.unlock();
    }
  }

  /**
   * Whether a partial-load rule is currently applied to this entry. Equivalent to {@code getRuleFingerprint() != null}.
   * While {@code true}, this entry holds a self-referential {@link StorageLocation.ReservationHold} that prevents cache
   * from evicting it, so the metadata's V10 header stays resident until {@link #clearRule()} runs.
   */
  public boolean isRuleHeld()
  {
    entryLock.lock();
    try {
      return ruleFingerprint != null;
    }
    finally {
      entryLock.unlock();
    }
  }

  /**
   * Apply (or replace) a partial-load rule on this entry. Must be called after {@link #mount(StorageLocation)}, the
   * entry must be registered with its {@link StorageLocation} so it can acquire holds on itself and on its bundles.
   * <p>
   * <b>Concurrency contract.</b> The caller MUST serialize {@code applyRule} and {@link #clearRule} for a given
   * segment id through an external per-segment lock ({@code SegmentLocalCacheManager} uses its
   * {@code ReferenceCountingLock segmentLocks}). This method releases {@link #entryLock} between Phase 2 (acquire
   * holds outside the lock) and Phase 4 (release holds outside the lock); a concurrent invocation of
   * {@code applyRule}/{@code clearRule} on the same entry can observe intermediate state during those windows and
   * silently miss rule-hold installations or leak them. {@link #entryLock} alone is insufficient because it is
   * released across the acquire/release phases by design (holding it across {@link StorageLocation} lock acquisition
   * would invert the writeLock &rarr; entryLock ordering the storage layer already uses for eviction).
   * <p>
   * On first application ({@code ruleFingerprint} transitions from {@code null}), a self-referential
   * {@link StorageLocation.ReservationHold} is taken to pin the metadata entry in-cache. On repeat calls with the same
   * {@code fingerprint} and matching {@code selectedBundleNames}, this is a no-op. On a genuine rule swap, only the
   * delta between the previous and new selection is applied: bundle holds for names dropped from the selection are
   * closed, and holds for names newly added are acquired for any bundle already registered with this entry (later
   * arrivals get their hold via {@link #registerBundle}).
   * <p>
   * Bundles whose {@link StorageLocation.ReservationHold} is currently zero-refcount (never mounted, or evicted but
   * unregistered before this call) will not appear in {@link #linkedBundles} yet, so no hold is taken for them here;
   * the caller is expected to drive an on-demand acquire per selected bundle name to trigger a fresh mount, which will
   * call {@link #registerBundle} and pick up the rule-hold at that point.
   *
   * @throws DruidException if the entry is not currently mounted
   */
  public void applyRule(String fingerprint, Set<String> selectedBundleNames)
  {
    Objects.requireNonNull(fingerprint, "fingerprint");
    Objects.requireNonNull(selectedBundleNames, "selectedBundleNames");
    final Set<String> newSelection = Set.copyOf(selectedBundleNames);

    // Phase 1: snapshot under entryLock and compute the diff. Do NOT call StorageLocation methods here — that would
    // acquire the location's readLock while holding entryLock and could deadlock with a concurrent writeLock holder
    // that needs entryLock
    final StorageLocation loc;
    final boolean needsSelfHold;
    final List<String> namesToRelease;
    final List<String> namesToAcquire;
    entryLock.lock();
    try {
      if (location == null) {
        throw DruidException.defensive(
            "applyRule on partial metadata entry[%s] requires the entry to be mounted",
            id
        );
      }
      if (fingerprint.equals(ruleFingerprint) && newSelection.equals(ruleSelectedBundleNames)) {
        return;
      }
      loc = location;
      needsSelfHold = (metadataSelfHold == null);
      namesToRelease = new ArrayList<>();
      for (String name : ruleBundleHolds.keySet()) {
        if (!newSelection.contains(name)) {
          namesToRelease.add(name);
        }
      }
      namesToAcquire = new ArrayList<>();
      for (String name : newSelection) {
        if (!ruleBundleHolds.containsKey(name) && findLinkedBundleByName(name) != null) {
          namesToAcquire.add(name);
        }
      }
    }
    finally {
      entryLock.unlock();
    }

    // Phase 2 + 3: acquire outside entryLock, then commit under entryLock. Track every hold this call acquires in
    // `uncommittedHolds`; when a hold's ownership transfers into `metadataSelfHold` or `ruleBundleHolds`, it is
    // removed from the list. Any throw between acquire and commit (or a race-lose at commit) leaves the hold in
    // `uncommittedHolds`, and the finally at Phase 4 releases it. This is what makes applyRule all-or-nothing at
    // the ReservationHold level: no leaks on partial failure, no stranded self-hold under a stale fingerprint.
    final List<StorageLocation.ReservationHold<?>> uncommittedHolds = new ArrayList<>();
    final List<StorageLocation.ReservationHold<?>> displacedHolds = new ArrayList<>();
    try {
      final StorageLocation.ReservationHold<PartialSegmentMetadataCacheEntry> newSelfHold;
      if (needsSelfHold) {
        newSelfHold = loc.addWeakReservationHoldIfExists(id);
        if (newSelfHold == null) {
          throw DruidException.defensive(
              "Failed to acquire self-referential rule-hold on partial metadata entry[%s]; entry is not weak-reserved",
              id
          );
        }
        uncommittedHolds.add(newSelfHold);
      } else {
        newSelfHold = null;
      }
      final Map<String, StorageLocation.ReservationHold<PartialSegmentBundleCacheEntry>> acquired = new HashMap<>();
      for (String name : namesToAcquire) {
        final StorageLocation.ReservationHold<PartialSegmentBundleCacheEntry> h =
            loc.addWeakReservationHoldIfExists(new PartialSegmentBundleCacheEntryIdentifier(segmentId, name));
        if (h != null) {
          acquired.put(name, h);
          uncommittedHolds.add(h);
        }
      }

      // Phase 3: commit under entryLock. Ownership transfers are done by removing from `uncommittedHolds` after
      // installing into the field. Anything left in `uncommittedHolds` at the end lost a race and gets released
      // in Phase 4 alongside `displacedHolds` (the pre-existing holds diffed out of ruleBundleHolds).
      entryLock.lock();
      try {
        if (newSelfHold != null) {
          if (metadataSelfHold == null) {
            metadataSelfHold = newSelfHold;
            uncommittedHolds.remove(newSelfHold);
          }
          // else: a concurrent applyRule installed a self-hold (shouldn't happen under segmentLock, but defensive).
          // newSelfHold stays in uncommittedHolds, gets released in Phase 4.
        }
        for (String name : namesToRelease) {
          final StorageLocation.ReservationHold<PartialSegmentBundleCacheEntry> h = ruleBundleHolds.remove(name);
          if (h != null) {
            displacedHolds.add(h);
          }
        }
        for (var e : acquired.entrySet()) {
          if (!ruleBundleHolds.containsKey(e.getKey())) {
            ruleBundleHolds.put(e.getKey(), e.getValue());
            uncommittedHolds.remove(e.getValue());
          }
          // else: a concurrent registerBundle raced ahead and installed a hold for this name; ours stays in
          // uncommittedHolds, gets released in Phase 4.
        }
        ruleFingerprint = fingerprint;
        ruleSelectedBundleNames = newSelection;
      }
      finally {
        entryLock.unlock();
      }
    }
    finally {
      // Phase 4: release outside entryLock. `uncommittedHolds` contains every hold this call acquired but did not
      // successfully transfer into a field (race-lose, mid-acquire failure, or commit failure). `displacedHolds`
      // contains pre-existing holds that the diff removed from ruleBundleHolds after successful commit.
      releaseHolds(uncommittedHolds);
      releaseHolds(displacedHolds);
    }
  }

  /**
   * Release the partial-load rule applied to this entry. Closes every {@link StorageLocation.ReservationHold} taken by
   * {@link #applyRule(String, Set)} (the metadata self-hold and every bundle hold) and clears the rule state. Bundle
   * entries that were kept resident only by the rule become eviction eligible; the metadata entry itself becomes
   * eviction eligible as well (unless a concurrent query holds a reference). Safe to call when no rule is applied, no-op.
   * <p>
   * <b>Concurrency contract.</b> Must be serialized against {@link #applyRule} for a given segment id via an external
   * per-segment lock (see {@code applyRule}'s Concurrency contract). Hold releases run OUTSIDE {@link #entryLock}, so
   * a concurrent {@code applyRule}/{@code clearRule} on the same entry can observe intermediate state.
   * <p>
   * Snapshot-under-lock, release-outside-lock. Closing a {@link StorageLocation.ReservationHold} eventually acquires
   * the location's writeLock (via {@code createWeakEntryReleaseRunnable}), so it must not run under {@link #entryLock}
   * lest it invert the writeLock &rarr; entryLock ordering the storage layer already relies on.
   */
  public void clearRule()
  {
    final List<StorageLocation.ReservationHold<?>> toClose = new ArrayList<>();
    entryLock.lock();
    try {
      toClose.addAll(ruleBundleHolds.values());
      ruleBundleHolds.clear();
      if (metadataSelfHold != null) {
        toClose.add(metadataSelfHold);
        metadataSelfHold = null;
      }
      ruleFingerprint = null;
      ruleSelectedBundleNames = Set.of();
    }
    finally {
      entryLock.unlock();
    }
    releaseHolds(toClose);
  }

  private void releaseHolds(List<StorageLocation.ReservationHold<?>> holds)
  {
    for (StorageLocation.ReservationHold<?> h : holds) {
      CloseableUtils.closeAndSuppressExceptions(
          h,
          t -> LOG.warn(t, "Failed releasing rule-hold on partial segment[%s]", segmentId)
      );
    }
  }

  @Nullable
  private PartialSegmentBundleCacheEntry findLinkedBundleByName(String bundleName)
  {
    return linkedBundles.get(bundleName);
  }

  /**
   * Whether every container belonging to {@code bundleName} has been downloaded.
   * <p>
   * Returns {@code false} when the entry is not mounted (no file mapper yet).
   */
  public boolean isBundleFullyDownloaded(String bundleName)
  {
    final PartialSegmentFileMapperV10 mapper = getFileMapper();
    return mapper != null && mapper.isBundleFullyDownloaded(bundleName);
  }

  /**
   * Whether {@code bundleName} is currently held by an applied partial-load rule on this entry (i.e. present in
   * {@link #ruleBundleHolds}).
   * <p>
   * NOT the same as "is the bundle linked": a bundle can be linked ({@link #linkedBundles}) without being rule-held,
   * because {@link #linkedBundles} is a lock-free {@link ConcurrentHashMap} keyset that
   * {@link #registerBundle}/{@link #unregisterBundle} mutate outside {@link #entryLock} (to preserve lock ordering
   * against the storage layer's writeLock &rarr; entryLock chain).
   */
  public boolean isBundleRuleHeld(String bundleName)
  {
    entryLock.lock();
    try {
      return ruleBundleHolds.containsKey(bundleName);
    }
    finally {
      entryLock.unlock();
    }
  }

  /**
   * Mount and eagerly download the named bundle. Drives an acquire through the internal
   * {@link #bundleAcquirer} (which mounts the bundle if needed, causing {@link #registerBundle} to fire), downloads
   * every container in the bundle via {@link PartialSegmentFileMapperV10#ensureBundleDownloaded}, then releases the
   * transient acquire hold. When a partial-load rule selects this bundle, the metadata's own rule-hold (installed by
   * {@link #registerBundle} on the mount that this call drives) keeps the bundle resident after the transient hold
   * releases. When no rule selects this bundle, the transient hold's release leaves the bundle eviction eligible; the
   * caller should already have driven {@link #applyRule} first to install the rule state before calling this.
   */
  public void ensureBundleResidentForRule(String bundleName) throws IOException
  {
    entryLock.lock();
    try {
      if (location == null || fileMapper == null) {
        throw DruidException.defensive(
            "ensureBundleResidentForRule on unmounted partial metadata entry[%s]",
            id
        );
      }
    }
    finally {
      entryLock.unlock();
    }
    final Closeable acquired = bundleAcquirer.acquire(bundleName);
    try {
      final PartialSegmentFileMapperV10 mapper = getFileMapper();
      if (mapper == null) {
        throw DruidException.defensive(
            "Partial metadata entry[%s] lost its file mapper mid-acquire for bundle[%s]",
            id,
            bundleName
        );
      }
      mapper.ensureBundleDownloaded(bundleName);
    }
    finally {
      CloseableUtils.closeAndSuppressExceptions(acquired, t -> LOG.warn(
          t,
          "Failed to release transient bundle-acquire hold for bundle[%s] on segment[%s]",
          bundleName,
          segmentId
      ));
    }
  }

  @Override
  public long getSize()
  {
    entryLock.lock();
    try {
      return currentSize;
    }
    finally {
      entryLock.unlock();
    }
  }

  @Override
  public boolean isMounted()
  {
    entryLock.lock();
    try {
      return fileMapper != null;
    }
    finally {
      entryLock.unlock();
    }
  }

  @Override
  public void resizeReservation(long newSize)
  {
    // Called from StorageLocation.adjustReservation under the location's writeLock. Acquires entryLock here as a
    // real (non-reentrant) acquisition: mount() releases entryLock BEFORE calling adjustReservation precisely so the
    // overall path runs writeLock -> entryLock (matching StorageLocation.release -> unmount), avoiding the
    // entryLock -> writeLock inversion that would deadlock.
    entryLock.lock();
    try {
      this.currentSize = newSize;
    }
    finally {
      entryLock.unlock();
    }
  }

  @Override
  public void mount(StorageLocation mountLocation) throws IOException
  {
    while (true) {
      final SettableFuture<Void> existing = mountFuture.get();
      if (existing != null) {
        awaitMount(existing);
        // The completed mount may have been for a different location. Verify the requested location matches.
        entryLock.lock();
        try {
          if (location != null && !location.equals(mountLocation)) {
            throw DruidException.defensive(
                "Already mounted[%s] in location[%s] which differs from requested[%s]",
                id,
                location.getPath(),
                mountLocation.getPath()
            );
          }
        }
        finally {
          entryLock.unlock();
        }
        verifyStillReservedOrRollback(mountLocation);
        return;
      }
      final SettableFuture<Void> ours = SettableFuture.create();
      if (!mountFuture.compareAndSet(null, ours)) {
        continue;
      }
      try {
        doMount(mountLocation);
        ours.set(null);
      }
      catch (Throwable t) {
        // Clear the future so the next caller gets a fresh attempt. Signal awaiters with the exception BEFORE firing
        // the onUnmount hook so any thread observing the failure via awaitMount(future) sees the exception before
        // observing the hook's file-system side effect (info-file deletion).
        mountFuture.set(null);
        ours.setException(t);
        runOnUnmountHookOnce();
        Throwables.propagateIfInstanceOf(t, IOException.class);
        Throwables.propagateIfPossible(t);
        throw DruidException.defensive(t, "Failed to mount metadata entry[%s]", id);
      }
      verifyStillReservedOrRollback(mountLocation);
      return;
    }
  }

  /**
   * Post-mount safety check: confirm the entry is still registered with the location, otherwise roll back. Handles
   * the race where the entry's reservation gets evicted (e.g. cache picks a weak entry whose lone hold was released
   * by a concurrent canceler, or {@link StorageLocation#release} fires on the static entry from a coordinator drop)
   * while mount() is still in progress. Without this check, mount would commit local state for an entry the cache
   * manager no longer knows about, leaking files on disk and memory mappings.
   */
  private void verifyStillReservedOrRollback(StorageLocation mountLocation)
  {
    if (!mountLocation.isReserved(id) && !mountLocation.isWeakReserved(id)) {
      LOG.debug(
          "Aborting mount of metadata entry[%s] in location[%s]; entry was evicted while mounting",
          id,
          mountLocation.getPath()
      );
      unmount();
    }
  }

  private void doMount(StorageLocation mountLocation) throws IOException
  {
    // The CAS+SettableFuture gate in mount() guarantees only one thread runs this method at a time per entry, so
    // entryLock is only held briefly for state mutations. The slow PartialSegmentFileMapperV10.create() call (which
    // may issue a deep-storage range read on first mount) runs outside entryLock so concurrent status reads are not
    // blocked on it. adjustReservation also runs outside entryLock: StorageLocation.release goes
    // writeLock -> entryLock (via release -> unmount), so entryLock -> writeLock here would be a deadlock-prone
    // lock-order inversion.
    try {
      entryLock.lock();
      try {
        if (location != null && fileMapper != null) {
          if (!location.equals(mountLocation)) {
            throw DruidException.defensive(
                "Already mounted[%s] in location[%s] which differs from requested[%s]",
                id,
                location.getPath(),
                mountLocation.getPath()
            );
          }
          return;
        }
      }
      finally {
        entryLock.unlock();
      }

      final PartialSegmentFileMapperV10 mapper = PartialSegmentFileMapperV10.create(
          rangeReader,
          jsonMapper,
          localCacheDir,
          targetFilename,
          externalFilenames,
          new WeakLoadTracker(mountLocation)
      );

      final long sizeToAdjust;
      try {
        final long actualSize = mapper.getOnDiskHeaderSize();
        if (actualSize > reservationEstimate) {
          throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                              .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                              .build(
                                  "Partial segment metadata for [%s] is [%d] bytes on disk, exceeding the "
                                  + "configured reservation estimate of [%d] bytes. Increase "
                                  + "druid.segmentCache.virtualStorageMetadataReservationEstimate.",
                                  segmentId,
                                  actualSize,
                                  reservationEstimate
                              );
        }
        sizeToAdjust = actualSize < reservationEstimate ? actualSize : -1;

        entryLock.lock();
        try {
          location = mountLocation;
          fileMapper = mapper;
          // Install (or re-install, after a previous mount/unmount cycle terminated the prior Phaser) the
          // reference-counted gate over cleanup. Future acquireMetadataReference() / unmount() calls operate on this
          // instance.
          references.set(new ReferenceCountingCloseableObject<Closeable>(this::doActualUnmount) {});
        }
        finally {
          entryLock.unlock();
        }
      }
      catch (Throwable t) {
        // mount failed; close mmaps and delete the on-disk header files so a retry starts clean. Mirrors the eager
        // SegmentCacheEntry behavior: simpler to redo a small header range-read than to reason about whatever partial
        // on-disk state the failure left. Crash-mid-mount across JVM restarts is still handled by the mapper's own
        // corruption recovery when bootstrap runs at next startup; this path covers the in-process retry case.
        try {
          mapper.close();
        }
        catch (Throwable closeError) {
          t.addSuppressed(closeError);
        }
        try {
          deleteHeaderFiles();
        }
        catch (Throwable deleteError) {
          t.addSuppressed(deleteError);
        }
        throw t;
      }

      // Only shrink the reservation if the entry is still registered with the location. If we lost the reservation
      // mid-mount (concurrent canceler / drop), adjustReservation would throw; defer to the post-mount check in
      // mount() to roll back cleanly instead.
      if (sizeToAdjust >= 0 && (mountLocation.isReserved(id) || mountLocation.isWeakReserved(id))) {
        mountLocation.adjustReservation(id, sizeToAdjust);
      }

      // Restore any bundles whose container files survived on disk for this segment. No-op on the fresh-acquire path
      // (the partialDir was just created and contains no container files yet). The bundle restore needs metadata's
      // file mapper, which is now installed, so this runs after the commit above. On any failure, fire our own
      // deferred-cleanup gate via unmount() to undo the file-mapper install + delete header files; synchronous since
      // we just installed the gate and no external caller has had a chance to acquire a reference yet. The location
      // reservation release stays the caller's responsibility (matches mount's overall contract).
      try {
        PartialSegmentCacheBootstrap.restoreBundlesFromDisk(this, mountLocation);
      }
      catch (Throwable t) {
        try {
          unmount();
        }
        catch (Throwable cleanupError) {
          t.addSuppressed(cleanupError);
        }
        Throwables.propagateIfInstanceOf(t, IOException.class);
        Throwables.propagateIfPossible(t);
        throw DruidException.defensive(t, "Failed to restore bundles for partial segment[%s]", segmentId);
      }
    }
    catch (Throwable t) {
      // A failed mount must not leave a lingering, un-re-mountable weak entry in the location. The inner rollbacks
      // above close the mapper and delete the on-disk header, so any weak entry left behind is poison: a later
      // findExistingPartialWithHold would resurrect it and re-mount would fail again (the header is gone and the
      // bootstrap reserve path's entry uses a disk-only range reader). Remove it here so the next acquire rebuilds a
      // fresh, deep-storage-capable entry via reservePartial. This is keyed on the entry being unheld: the runtime
      // acquire path holds the entry via the AcquireSegmentAction's loadCleanup, so removeUnheldWeakEntry is a no-op
      // there and the holder's release runnable performs cleanup instead. The bootstrap reserve path
      // (StorageLocation.reserveWeak) places no hold, so this is what cleans it up. Runs outside entryLock (the
      // inner blocks released it) so the writeLock -> entryLock order inside removeUnheldWeakEntry is respected.
      try {
        mountLocation.removeUnheldWeakEntry(id);
      }
      catch (Throwable removeError) {
        t.addSuppressed(removeError);
      }
      throw t;
    }
  }

  private static void awaitMount(SettableFuture<Void> future) throws IOException
  {
    try {
      future.get();
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while waiting for mount", e);
    }
    catch (ExecutionException e) {
      final Throwable cause = e.getCause() == null ? e : e.getCause();
      switch (cause) {
        case IOException ioException -> throw ioException;
        case RuntimeException runtimeException -> throw runtimeException;
        case Error error -> throw error;
        default -> throw DruidException.defensive(e, "mount failed");
      }
    }
  }

  /**
   * Triggers cleanup of this entry. If any references acquired via {@link #acquireMetadataReference()} are still
   * outstanding, the actual unmap-and-delete work is deferred until the last reference releases; in that case this
   * method returns immediately and {@link #doActualUnmount} will fire later on the thread that closes the last
   * reference. With no outstanding references, cleanup runs synchronously on the caller's thread.
   * <p>
   * If this entry was reserved but never mounted, there is no reference-counted gate to close; instead run the
   * {@link #setOnUnmount onUnmount} hook directly so external cleanup (e.g. info-file deletion) still fires.
   * <p>
   * <b>Mid-mount race guard.</b> A concurrent reclaim can invoke {@code unmount()} while {@link #doMount} is still
   * running its slow deep-storage header fetch and has not yet installed {@link #references}. Firing the hook here
   * would race with {@code doMount}. Detect this via {@link #mountFuture} (set by {@link #mount} before
   * {@code doMount} runs; cleared by {@code doMount}'s failure path and by {@link #doActualUnmount}) and defer
   * hook-firing to the mount path: on success {@code verifyStillReservedOrRollback} calls {@code unmount()} again
   * with {@link #references} installed and the hook fires via {@link #doActualUnmount}; on failure {@link #mount}'s
   * catch block fires the hook directly (see below).
   */
  @Override
  public void unmount()
  {
    final ReferenceCountingCloseableObject<Closeable> current = references.get();
    if (current != null && !current.isClosed()) {
      current.close();
      return;
    }
    if (mountFuture.get() != null) {
      // doMount is in flight and has not yet installed `references`; hook-firing is deferred to the mount path.
      return;
    }
    // Never-mounted (or already-completed-cleanup) release: doActualUnmount won't run, so fire the hook here.
    runOnUnmountHookOnce();
  }

  /**
   * Atomically extract and run the {@link #setOnUnmount onUnmount} hook if one is set. Idempotent across concurrent
   * callers, the {@code getAndSet(null)} ensures exactly one caller ever observes a non-null hook.
   */
  private void runOnUnmountHookOnce()
  {
    final Runnable hook = onUnmount.getAndSet(null);
    if (hook != null) {
      try {
        hook.run();
      }
      catch (Throwable t) {
        LOG.warn(t, "onUnmount hook failed for partial segment metadata entry[%s]", segmentId);
      }
    }
  }

  /**
   * Acquire a reference that keeps this entry's resources (the file mapper, on-disk header files) alive across an
   * intervening {@link #unmount} call. The returned {@link Closeable} must be closed when the caller is done; at
   * that point if {@code unmount()} has already been called and no other references remain, the deferred cleanup
   * fires on the closing thread.
   * <p>
   * This is the metadata-layer refcount used by {@link PartialSegmentBundleCacheEntry} (to keep the file mapper alive
   * across an intervening drop) and by {@link #acquireReference} (which acquires one of these refs for each
   * {@link PartialQueryableIndexSegment} it hands out). Callers that just want a {@link Segment} should use
   * {@link #acquireReference} instead.
   *
   * @throws DruidException if the entry has never been mounted, or has already been cleaned up
   */
  public Closeable acquireMetadataReference()
  {
    final ReferenceCountingCloseableObject<Closeable> current = references.get();
    if (current == null) {
      throw DruidException.defensive(
          "Cannot acquire reference on partial segment metadata entry[%s] before it has been mounted",
          id
      );
    }
    return current.incrementReferenceAndDecrementOnceCloseable()
                  .orElseThrow(() -> DruidException.defensive(
                      "Cannot acquire reference on partial segment metadata entry[%s]; already being unmounted",
                      id
                  ));
  }

  /**
   * Build a {@link Segment} backed by this entry's mounted file mapper. Returns {@link Optional#empty()} when the
   * entry isn't mounted. The segment internally acquires one {@link #acquireMetadataReference} so that closing the
   * segment is what releases the metadata-layer reference.
   *
   * @throws DruidException if no {@code storagePool} was supplied at construction time (this entry is
   *                        metadata-only and cannot produce a queryable segment)
   */
  @Override
  public Optional<Segment> acquireReference()
  {
    return buildSegmentWithExtraClose(null);
  }

  /**
   * Partial entries compose extra closeables natively into the segment's single {@code onClose} hook via a
   * {@link Closer}.
   */
  @Override
  public Optional<Segment> acquireReference(Closeable extraOnClose)
  {
    return buildSegmentWithExtraClose(extraOnClose);
  }

  private Optional<Segment> buildSegmentWithExtraClose(@Nullable Closeable extraOnClose)
  {
    final PartialQueryableIndex index;
    entryLock.lock();
    try {
      if (fileMapper == null) {
        CloseableUtils.closeAndSuppressExceptions(extraOnClose, ignored -> {});
        return Optional.empty();
      }
      if (queryableIndex == null) {
        queryableIndex = new PartialQueryableIndex(
            fileMapper.getSegmentFileMetadata(),
            fileMapper,
            ColumnConfig.DEFAULT
        );
      }
      index = queryableIndex;
    }
    finally {
      entryLock.unlock();
    }
    if (!hasStoragePool()) {
      CloseableUtils.closeAndSuppressExceptions(extraOnClose, ignored -> {});
      throw DruidException.defensive(
          "Cannot build segment for partial entry[%s]; no storage loading thread pool was supplied at construction",
          id
      );
    }
    final Closeable metadataRef;
    try {
      metadataRef = acquireMetadataReference();
    }
    catch (DruidException raceLost) {
      // Defensive: acquireMetadataReference only fails if the entry is mid-unmount, but every caller of
      // acquireReference holds a reservation hold on this entry across the call, and a held weak entry can't be
      // concurrently unmounted, so this branch is unreachable today
      CloseableUtils.closeAndSuppressExceptions(extraOnClose, ignored -> {});
      return Optional.empty();
    }
    // Compose metadata-ref + optional extra into a single Closer so the segment's single-Closeable onClose contract
    // stays simple. The Closer suppresses subsequent failures into the first, so every hook gets a chance to run.
    final Closer onClose = Closer.create();
    if (extraOnClose != null) {
      onClose.register(extraOnClose);
    }
    onClose.register(metadataRef);
    return Optional.of(
        new PartialQueryableIndexSegment(
            index,
            segmentId,
            onClose,
            bundleAcquirer
        )
    );
  }

  /**
   * Acquire a fully-materialized reference for {@link AcquireMode#FULL} with the metadata reference plus a
   * hold on every bundle, all folded into the returned segment's close so the segment stays fully resident for its
   * lifetime.
   * <p>
   * Returns {@link Optional#empty()} when the segment is not fully resident in the cache.
   */
  public Optional<Segment> acquireFullReference(@Nullable Closeable extraOnClose)
  {
    final PartialSegmentFileMapperV10 mapper = getFileMapper();
    if (mapper == null || !isFullyDownloaded()) {
      CloseableUtils.closeAndSuppressExceptions(extraOnClose, ignored -> {});
      return Optional.empty();
    }
    final Closer bundleHolds = Closer.create();
    try {
      // acquire() mounts each bundle's parents first, so iteration order doesn't matter. When the entry is resident
      // every bundle is already mounted, so each acquire is a cheap refcount; only the rare eviction race below does
      // any work (a discarded empty sparse-mount).
      for (String bundleName : PartialSegmentBundleCacheEntry.bundleNames(mapper)) {
        bundleHolds.register(bundleAcquirer.acquire(bundleName));
      }
      // Re-check under the holds: a bundle could have been evicted between the check above and taking its hold,
      // in which case acquire() re-mounted it as an empty container and isFullyDownloaded() is now false. Bail so the
      // caller downloads via the executor path rather than handing back a segment that fails in makeCursorHolder.
      if (!isFullyDownloaded()) {
        CloseableUtils.closeAndSuppressExceptions(bundleHolds, ignored -> {});
        CloseableUtils.closeAndSuppressExceptions(extraOnClose, ignored -> {});
        return Optional.empty();
      }
    }
    catch (Throwable t) {
      CloseableUtils.closeAndSuppressExceptions(bundleHolds, ignored -> {});
      CloseableUtils.closeAndSuppressExceptions(extraOnClose, ignored -> {});
      throw t;
    }
    final Closer onClose = Closer.create();
    if (extraOnClose != null) {
      onClose.register(extraOnClose);
    }
    onClose.register(bundleHolds);
    return acquireReference(onClose);
  }

  /**
   * The actual unmount work, invoked by the reference-counted gate's {@code onAdvance} once every outstanding
   * reference (plus the wrapper's own initial party) has been released. Closes the file mapper, deletes the on-disk
   * header files (the entry owns its storage-location footprint), and runs the optional {@link #setOnUnmount
   * onUnmount} hook.
   */
  private void doActualUnmount()
  {
    entryLock.lock();
    try {
      if (fileMapper == null) {
        return;
      }
      // doActualUnmount must never fire while a partial-load rule is applied.
      if (ruleFingerprint != null) {
        throw DruidException.defensive(
            "doActualUnmount fired for partial segment[%s] with rule state still applied (fingerprint=%s, "
            + "selectedBundles=%s); the metadata self-hold invariant has been broken",
            segmentId,
            ruleFingerprint,
            ruleSelectedBundleNames
        );
      }
      try {
        fileMapper.close();
      }
      catch (Throwable t) {
        LOG.warn(t, "Failed to close partial segment file mapper for [%s]", segmentId);
      }
      fileMapper = null;
      // Drop the cached PartialQueryableIndex; the file mapper it referenced is now closed. A subsequent mount
      // rebuilds it against a fresh mapper.
      queryableIndex = null;
      location = null;
      // Clear the mount-dedup gate so a subsequent mount() on this same instance starts a fresh attempt.
      mountFuture.set(null);
      deleteHeaderFiles();
    }
    finally {
      entryLock.unlock();
    }
    // Run the hook outside entryLock so it can touch the file system / cache manager without contending with
    // concurrent status reads, and so a slow or buggy hook can't deadlock against acquireReference paths.
    runOnUnmountHookOnce();
  }

  /**
   * Whether every internal file referenced by this segment's metadata (including any attached external mappers) has
   * been downloaded. Delegates to {@link PartialSegmentFileMapperV10#isFullyDownloaded}. Returns false when the
   * entry is not mounted.
   */
  @Override
  public boolean isFullyDownloaded()
  {
    entryLock.lock();
    try {
      return fileMapper != null && fileMapper.isFullyDownloaded();
    }
    finally {
      entryLock.unlock();
    }
  }

  /**
   * Returns the file mapper held by this entry while mounted, or null if the entry has not been mounted.
   */
  @Nullable
  public PartialSegmentFileMapperV10 getFileMapper()
  {
    entryLock.lock();
    try {
      return fileMapper;
    }
    finally {
      entryLock.unlock();
    }
  }

  /**
   * Returns the parsed segment file metadata while mounted, or null if not yet mounted.
   */
  @Nullable
  public SegmentFileMetadata getSegmentFileMetadata()
  {
    final PartialSegmentFileMapperV10 mapper = getFileMapper();
    return mapper == null ? null : mapper.getSegmentFileMetadata();
  }

  /**
   * Build the {@link PartialBundleAcquirer} cached at construction time on {@link #bundleAcquirer}. The acquirer the
   * partial-aware cursor factory uses after projection matching looks up an existing bundle entry by name on this
   * metadata's storage location, creates + reserves a fresh one when none exists, mounts it (idempotent), and
   * returns a per-cursor handle holding two protections:
   * <ul>
   *   <li>a {@link StorageLocation.ReservationHold} so the cache doesn't evict the bundle while the cursor is reading,
   *       and</li>
   *   <li>a {@link PartialSegmentBundleCacheEntry#acquireReference reference} on the bundle entry so an explicit
   *       drop / unmount defers its actual cleanup until the cursor finishes.</li>
   * </ul>
   * Closing the returned {@link Closeable} releases both. Once the last hold drops the bundle becomes eviction-eligible
   * and is unlinked from the location naturally on the next reclaim.
   */
  @VisibleForTesting
  PartialBundleAcquirer getBundleAcquirer()
  {
    return bundleAcquirer;
  }

  /**
   * Whether a usable {@link StorageLoadingThreadPool} was supplied at construction. When false, this entry can serve
   * schema/metadata but cannot mount bundles or submit column-load tasks (e.g. a metadata-only stub entry).
   */
  private boolean hasStoragePool()
  {
    return storagePool != null && storagePool.isAvailable();
  }

  private PartialBundleAcquirer createBundleAcquirer()
  {
    return new PartialBundleAcquirer()
    {
      @Override
      public <T> AsyncResource<T> submitDownload(Callable<T> task)
      {
        if (!hasStoragePool()) {
          throw DruidException.defensive(
              "No storage loading thread pool was supplied at construction for partial entry[%s]; cannot serve "
              + "column-load tasks",
              id
          );
        }
        return storagePool.submitUnmanagedAsyncResource(task);
      }

      @Override
      public Closeable acquire(String requestedBundleName)
      {
        final StorageLocation loc;
        final PartialSegmentFileMapperV10 mapper;
        entryLock.lock();
        try {
          loc = location;
          mapper = fileMapper;
        }
        finally {
          entryLock.unlock();
        }
        if (loc == null || mapper == null) {
          // Programming error: acquire shouldn't be called on an unmounted metadata entry. The cache manager only
          // hands out segments produced by acquireReference(), which only succeeds after mount.
          throw DruidException.defensive(
              "Metadata entry for [%s] is not mounted; cannot acquire bundle[%s]",
              segmentId,
              requestedBundleName
          );
        }

        // Map the requested projection/base bundle name onto an actual on-disk bundle. Normally identity, but a
        // legacy/untagged segment (sole bundle is the root bundle) resolves any request to the root catch-all.
        final String bundleName = PartialSegmentBundleCacheEntry.resolveBundleName(mapper, requestedBundleName);
        final PartialSegmentBundleCacheEntryIdentifier bundleId =
            new PartialSegmentBundleCacheEntryIdentifier(segmentId, bundleName);

        // Take the hold atomically: reuse an existing entry if present, otherwise build + reserve a fresh one. Both
        // addWeakReservationHold* variants take the hold under the location lock, so there is no getCacheEntry-then-
        // hold race window. addWeakReservationHold is atomic create-or-get, so a concurrent acquire that raced in
        // returns the same entry rather than a duplicate. And because a held entry can't be concurrently unmounted
        // (SIEVE reclaim and removeUnheldWeakEntry both skip held entries, and the metadata cascade is gated by the
        // bundle's reference on the metadata), the subsequent mount + acquireReference can't lose a drop race.
        final StorageLocation.ReservationHold<PartialSegmentBundleCacheEntry> existingHold =
            loc.addWeakReservationHoldIfExists(bundleId);
        final StorageLocation.ReservationHold<PartialSegmentBundleCacheEntry> hold =
            existingHold != null
            ? existingHold
            : loc.addWeakReservationHold(
                bundleId,
                () -> PartialSegmentBundleCacheEntry.forBundle(
                    PartialSegmentMetadataCacheEntry.this,
                    bundleName,
                    inferBundleDependencies(bundleName)
                )
            );
        if (hold == null) {
          throw DruidException.forPersona(DruidException.Persona.USER)
                              .ofCategory(DruidException.Category.CAPACITY_EXCEEDED)
                              .build(
                                  "Unable to reserve bundle[%s] for segment[%s]; ensure enough disk space has been "
                                  + "allocated to load all segments involved in the query",
                                  bundleName,
                                  segmentId
                              );
        }

        final PartialSegmentBundleCacheEntry bundle = hold.getEntry();
        if (!bundle.isMounted()) {
          // Freshly built, or a stale entry from a prior cycle whose cleanup ran: (re-)mount it. mount() is idempotent
          // via its mount-future dedup, so a concurrent acquirer that already mounted it makes this a cheap no-op.
          mountBundleOrClose(bundle, loc, hold, bundleName);
        }

        // Acquire the bundle's own reference, which gates its deferred cleanup on explicit unmount calls. The hold and
        // the reference must close together when the cursor closes; they protect against different eviction triggers
        // (SIEVE eviction vs. explicit drop cascade).
        final Closeable bundleRef;
        try {
          bundleRef = bundle.acquireReference();
        }
        catch (Throwable t) {
          throw CloseableUtils.closeAndWrapInCatch(t, hold);
        }

        return () -> {
          try {
            bundleRef.close();
          }
          finally {
            hold.close();
          }
        };
      }

      /**
       * Mount the bundle on the supplied location, closing the bootstrap hold and propagating a {@link DruidException}
       * if the mount fails. Used by both the fresh-build and re-mount branches.
       */
      private void mountBundleOrClose(
          PartialSegmentBundleCacheEntry bundle,
          StorageLocation loc,
          StorageLocation.ReservationHold<PartialSegmentBundleCacheEntry> hold,
          String bundleName
      )
      {
        // Mount this bundle's parents (e.g. __base for a projection bundle) FIRST: the bundle's own mount() takes
        // holds + references on each parent and fails with a defensive error if a parent isn't registered+mounted at
        // the location. The bootstrap restore path orders base-before-dependents; this is the equivalent ordering for
        // the runtime acquire path, which would otherwise reach a projection bundle directly with no __base mounted.
        // The bundle keeps its own dependency holds/refs for its lifetime, so we hold these transient ones only across
        // the mount and release them immediately after a successful mount (acquire() is acyclic: base/root have no
        // dependencies, so the recursion terminates).
        final Closer parentHolds = Closer.create();
        try {
          for (PartialSegmentBundleCacheEntryIdentifier depId : inferBundleDependencies(bundleName)) {
            parentHolds.register(acquire(depId.bundleName()));
          }
          bundle.mount(loc);
        }
        catch (Throwable t) {
          CloseableUtils.closeAndSuppressExceptions(hold, t::addSuppressed);
          CloseableUtils.closeAndSuppressExceptions(parentHolds, t::addSuppressed);
          throw DruidException.defensive(t, "Failed to mount bundle[%s] for segment[%s]", bundleName, segmentId);
        }
        CloseableUtils.closeAndSuppressExceptions(
            parentHolds,
            e -> LOG.warn(e, "Failed to release transient parent holds after mounting bundle[%s]", bundleName)
        );
      }
    };
  }

  /**
   * The bundles that {@code bundleName} depends on within this segment. Depending here means: at mount time, this
   * bundle's file mapper must be able to acquire a hold on each dependency, and the dependency's containers must
   * remain resident for as long as this bundle is mounted.
   * <p>
   * The rule is uniform: the base bundle and the {@link SegmentFileBuilder#ROOT_BUNDLE_NAME root bundle} have no
   * dependencies (the root bundle owns everything written without an explicit {@code startFileBundle} call, for
   * older fileGroup-less segments, or any future shared internal metadata and is structurally a peer of the base);
   * every other bundle depends on the base bundle, but only if this segment actually carries one.
   * <p>
   * If future writers introduce richer dependency graphs, the rule will need to grow, likely by reading dependency
   * metadata the writer records explicitly.
   */
  public List<PartialSegmentBundleCacheEntryIdentifier> inferBundleDependencies(String bundleName)
  {
    if (Projections.BASE_TABLE_PROJECTION_NAME.equals(bundleName)
        || SegmentFileBuilder.ROOT_BUNDLE_NAME.equals(bundleName)
        || !hasBaseBundle()) {
      return List.of();
    }
    return List.of(
        new PartialSegmentBundleCacheEntryIdentifier(
            segmentId,
            Projections.BASE_TABLE_PROJECTION_NAME
        )
    );
  }

  /**
   * Return {@code roots} plus every transitive dependency (as inferred by {@link #inferBundleDependencies}) in
   * dependency-first order.
   * <p>
   * No cycle guard: {@link #inferBundleDependencies} only returns {@code []} or {@code [__base]}, and {@code __base}
   * itself has no dependencies, so the graph is at most one level deep by construction. A richer future dependency
   * graph should revisit this.
   */
  public List<String> bundlesInMountOrder(Iterable<String> roots)
  {
    final LinkedHashSet<String> ordered = new LinkedHashSet<>();
    for (String root : roots) {
      visitDependenciesFirst(root, ordered);
    }
    return List.copyOf(ordered);
  }

  private void visitDependenciesFirst(String bundleName, LinkedHashSet<String> ordered)
  {
    if (ordered.contains(bundleName)) {
      return;
    }
    for (PartialSegmentBundleCacheEntryIdentifier dep : inferBundleDependencies(bundleName)) {
      visitDependenciesFirst(dep.bundleName(), ordered);
    }
    ordered.add(bundleName);
  }

  /**
   * Whether this segment carries a {@code __base} bundle (shared base-table column data). Probed from the mounted file
   * mapper's actual bundle set; returns false when the entry is not mounted.
   */
  private boolean hasBaseBundle()
  {
    final PartialSegmentFileMapperV10 mapper = getFileMapper();
    return mapper != null
           && PartialSegmentBundleCacheEntry.bundleNames(mapper).contains(Projections.BASE_TABLE_PROJECTION_NAME);
  }

  /**
   * Register a bundle entry as a current dependent of this metadata entry. Called by
   * {@link PartialSegmentBundleCacheEntry} after a successful mount; the drop path uses {@link #snapshotLinkedBundles}
   * to enumerate dependents for cascade-close.
   * <p>
   * If a partial-load rule is currently applied and {@code bundle}'s name is in the rule's selection set, take a
   * {@link StorageLocation.ReservationHold} on the bundle so it stays resident until the rule is cleared or swapped
   * away. The hold acquisition uses a snapshot-under-lock, acquire-outside-lock, commit-under-lock pattern to avoid
   * holding {@link #entryLock} while calling into {@link StorageLocation}'s read/write locks.
   */
  void registerBundle(PartialSegmentBundleCacheEntry bundle)
  {
    final String bundleName = bundle.getBundleName();
    linkedBundles.put(bundleName, bundle);

    // Phase 1: snapshot under entryLock.
    final StorageLocation loc;
    entryLock.lock();
    try {
      if (location == null
          || !ruleSelectedBundleNames.contains(bundleName)
          || ruleBundleHolds.containsKey(bundleName)) {
        return;
      }
      loc = location;
    }
    finally {
      entryLock.unlock();
    }

    // Phase 2: acquire outside entryLock. Caller's mount path (via bundleAcquirer.acquire) already holds a transient
    // ReservationHold on this bundle, so the weak entry is guaranteed present and this acquire cannot return null
    // for a "just evicted" reason.
    final StorageLocation.ReservationHold<PartialSegmentBundleCacheEntry> hold =
        loc.addWeakReservationHoldIfExists(new PartialSegmentBundleCacheEntryIdentifier(segmentId, bundleName));
    if (hold == null) {
      return;
    }

    // Phase 3: commit under entryLock, or discard if the rule state moved under us.
    boolean stored = false;
    entryLock.lock();
    try {
      if (ruleSelectedBundleNames.contains(bundleName) && !ruleBundleHolds.containsKey(bundleName)) {
        ruleBundleHolds.put(bundleName, hold);
        stored = true;
      }
    }
    finally {
      entryLock.unlock();
    }
    if (!stored) {
      CloseableUtils.closeAndSuppressExceptions(
          hold,
          t -> LOG.warn(t, "Failed releasing discarded rule-hold for bundle[%s] on segment[%s]", bundleName, segmentId)
      );
    }
  }

  /**
   * Reverse of {@link #registerBundle}. Called by {@link PartialSegmentBundleCacheEntry#unmount} so the metadata's
   * view stays consistent with which bundles are actually mounted. Bookkeeping-only: never touches {@link
   * #ruleBundleHolds}.
   */
  void unregisterBundle(PartialSegmentBundleCacheEntry bundle)
  {
    // Remove only if the exact same instance is still registered under this name. A late-arriving unregister for a
    // stale bundle instance (e.g. one that was replaced by a fresh remount) must not evict the fresh instance.
    linkedBundles.remove(bundle.getBundleName(), bundle);
  }

  /**
   * Snapshot of bundle entries currently mounted against this segment. Returned as a defensive copy; callers can
   * iterate freely without risk of concurrent-modification surprises while bundles concurrently mount/unmount. Used
   * by the drop path to cascade-close bundles before releasing the metadata entry.
   */
  public Collection<PartialSegmentBundleCacheEntry> snapshotLinkedBundles()
  {
    return new ArrayList<>(linkedBundles.values());
  }

  /**
   * Attach a deferred-cleanup hook to run when this entry is finally purged. {@link #doActualUnmount} invokes the
   * hook after closing the file mapper and deleting the entry's storage-location files, outside the entry lock.
   * Replaces any previously-set hook. Pass {@code null} to clear.
   */
  @Override
  public void setOnUnmount(@Nullable Runnable hook)
  {
    onUnmount.set(hook);
  }

  /**
   * Delete the on-disk header files this entry owns (main + any externals). Called from both
   * {@link #doActualUnmount} on successful purge and the mount-failure cleanup path; safe to invoke independently of
   * mount state.
   */
  private void deleteHeaderFiles()
  {
    deleteIfExists(new File(localCacheDir, targetFilename + PartialSegmentFileMapperV10.METADATA_HEADER_SUFFIX));
    for (String filename : externalFilenames) {
      deleteIfExists(new File(localCacheDir, filename + PartialSegmentFileMapperV10.METADATA_HEADER_SUFFIX));
    }
  }

  private void deleteIfExists(File file)
  {
    if (file.exists() && !file.delete()) {
      LOG.warn("Failed to delete header file[%s] during unmount of partial segment[%s]", file, segmentId);
    }
  }

  private record WeakLoadTracker(StorageLocation mountLocation) implements PartialSegmentDownloadListener
  {
    @Override
    public void onBytesDownloaded(long bytes)
    {
      mountLocation.trackWeakLoad(bytes);
    }

    @Override
    public void onRangeRead(long bytes, long nanos)
    {
      mountLocation.trackWeakRangeRead(bytes, nanos);
    }
  }
}
