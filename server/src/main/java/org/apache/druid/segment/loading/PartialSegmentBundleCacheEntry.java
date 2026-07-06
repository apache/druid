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

import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.ReferenceCountingCloseableObject;
import org.apache.druid.segment.file.PartialSegmentFileMapperV10;
import org.apache.druid.segment.file.SegmentFileBuilder;
import org.apache.druid.segment.file.SegmentFileContainerMetadata;
import org.apache.druid.segment.file.SegmentFileMetadata;
import org.apache.druid.timeline.SegmentId;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Cache entry for a single named bundle within a partial-loaded V10 segment. A bundle is a group of containers
 * declared at write time via {@link SegmentFileBuilder#startFileBundle}; the cache layer reads each container's
 * {@link SegmentFileContainerMetadata#getBundle} field and treats every container in the named bundle as one
 * mount/evict unit. Containers written without an explicit {@code startFileBundle} call (and containers from older
 * segments that did not carry a bundle name) default to {@link SegmentFileBuilder#ROOT_BUNDLE_NAME}, the implicit
 * root bundle that sits above any named ones. A bundle may also span multiple physical files: when the writer
 * propagates {@code startFileBundle} to attached external segment files, the cache layer transparently includes their
 * matching containers in the same bundle.
 * <p>
 * Mounting a bundle entry sparse-allocates its containers locally via
 * {@link PartialSegmentFileMapperV10#initializeContainer}; unmounting evicts them (unmap + delete + clear bitmap) via
 * {@link PartialSegmentFileMapperV10#evictContainer}.
 * <p>
 * <b>Dependency holds + references.</b> A bundle entry holds two layers of protection on its metadata cache entry
 * plus every transitive parent bundle entry passed in at construction time via {@code parentEntryIds}. The first
 * layer is a {@link StorageLocation.ReservationHold} acquired via
 * {@link StorageLocation#addWeakReservationHoldIfExists}, which prevents evicting weak dependencies while this bundle
 * is mounted (no-op for statically reserved dependencies). The second layer is a reference acquired via
 * {@link PartialSegmentMetadataCacheEntry#acquireReference} / {@link #acquireReference} on each dependency, which
 * defers each dependency's actual unmap-and-delete work until this bundle's own {@link #unmount} runs; this is the
 * protection that matters for statically reserved dependencies where the cache hold is no-op. Both are acquired
 * during {@link #mount} and released during {@link #unmount}; if a parent is missing or cannot be acquired, mount
 * fails and any holds/references already taken are released.
 * <p>
 * <b>Reference-counted deferred cleanup of this bundle.</b> {@link #unmount()} doesn't necessarily release resources
 * synchronously. While any references acquired via {@link #acquireReference()} are outstanding (e.g. an in-flight
 * cursor reading this bundle's columns), the actual evict-containers-and-release-dependencies work is deferred until
 * the last reference releases. The same instance can be re-mounted after a previous cleanup completes; a fresh
 * internal Phaser is installed on the next successful mount.
 * <p>
 * <b>Mount-time dedup.</b> Concurrent {@link #mount} calls are deduplicated via a {@link AtomicReference} of
 * {@link SettableFuture}; one thread does the work, the rest wait on the same future. On failure the gate is
 * cleared so a subsequent caller gets a fresh attempt; on success the gate stays set until {@link #unmount}.
 */
public class PartialSegmentBundleCacheEntry implements CacheEntry
{
  private static final EmittingLogger LOG = new EmittingLogger(PartialSegmentBundleCacheEntry.class);

  /**
   * Build a bundle entry given an already-mounted metadata entry and the bundle's name (as declared at write time via
   * {@link SegmentFileBuilder#startFileBundle}, or implicitly {@link SegmentFileBuilder#ROOT_BUNDLE_NAME} for
   * containers written without an explicit call). Walks the main file's containers plus each attached external
   * file's containers, picking every container whose {@link SegmentFileContainerMetadata#getBundle bundle} equals
   * {@code bundleName}.
   */
  public static PartialSegmentBundleCacheEntry forBundle(
      PartialSegmentMetadataCacheEntry metadataEntry,
      String bundleName,
      List<PartialSegmentBundleCacheEntryIdentifier> parentEntryIds
  )
  {
    final PartialSegmentFileMapperV10 fileMapper = metadataEntry.getFileMapper();
    if (fileMapper == null) {
      throw DruidException.defensive(
          "Cannot create bundle entry for [%s/%s]: metadata entry is not mounted",
          metadataEntry.getSegmentId(),
          bundleName
      );
    }

    final List<BundleContainerRef> refs = findContainersForBundle(fileMapper, bundleName);
    if (refs.isEmpty()) {
      throw DruidException.defensive(
          "Bundle[%s] has no containers in segment[%s]",
          bundleName,
          metadataEntry.getSegmentId()
      );
    }

    long size = 0;
    for (BundleContainerRef ref : refs) {
      size += fileMapper.mapperForContainer(ref.externalFilename())
                        .getSegmentFileMetadata()
                        .getContainers()
                        .get(ref.containerIndex())
                        .getSize();
    }

    return new PartialSegmentBundleCacheEntry(
        metadataEntry.getSegmentId(),
        bundleName,
        refs,
        size,
        metadataEntry,
        List.copyOf(parentEntryIds)
    );
  }

  /**
   * Find every {@link BundleContainerRef} that the named bundle owns across the main file and each external file:
   * any container whose {@link SegmentFileContainerMetadata#getBundle bundle} equals {@code bundleName}. Shared by
   * {@link #forBundle} and the bootstrap path so both observe the same definition of bundle membership.
   */
  public static List<BundleContainerRef> findContainersForBundle(
      PartialSegmentFileMapperV10 fileMapper,
      String bundleName
  )
  {
    final List<BundleContainerRef> refs = new ArrayList<>();
    for (int containerIndex : fileMapper.getContainerIndicesForBundle(bundleName)) {
      refs.add(new BundleContainerRef(null, containerIndex));
    }
    for (String externalFilename : fileMapper.getExternalFilenames()) {
      for (int containerIndex : fileMapper.getExternalMapper(externalFilename).getContainerIndicesForBundle(bundleName)) {
        refs.add(new BundleContainerRef(externalFilename, containerIndex));
      }
    }
    return List.copyOf(refs);
  }

  private final PartialSegmentBundleCacheEntryIdentifier id;
  private final SegmentId segmentId;
  private final String bundleName;
  private final List<BundleContainerRef> containerRefs;
  private final long size;
  private final PartialSegmentMetadataCacheEntry metadataEntry;
  private final List<PartialSegmentBundleCacheEntryIdentifier> parentEntryIds;

  private final ReentrantLock entryLock = new ReentrantLock();
  private final AtomicReference<SettableFuture<Void>> mountFuture = new AtomicReference<>();

  @GuardedBy("entryLock")
  @Nullable
  private StorageLocation location;
  @GuardedBy("entryLock")
  private final List<StorageLocation.ReservationHold<?>> holds = new ArrayList<>();
  // references this bundle holds on its metadata entry and each transitive parent bundle for the duration of its
  // mounted lifetime. Released in doActualUnmount. Distinct from `holds` (cache-eviction protection): these references
  // gate deferred cleanup on the dependencies, so an in-flight query that holds a reference on this bundle keeps
  // metadata + parents safe from drop-time unmap even if the dependency is statically reserved.
  @GuardedBy("entryLock")
  private final List<Closeable> dependencyReferences = new ArrayList<>();
  @GuardedBy("entryLock")
  private boolean mounted;

  // Reference-counted gate over the actual cleanup work (evict containers, release parent holds, unregister from
  // metadata). Set on successful mount; unmount() closes the wrapper which defers running cleanup until all outstanding
  // references (acquired via acquireReference()) are released. Re-created on mount-after-cleanup-completion. Null when
  // the entry has never been mounted.
  private final AtomicReference<ReferenceCountingCloseableObject<Closeable>> references = new AtomicReference<>();

  PartialSegmentBundleCacheEntry(
      SegmentId segmentId,
      String bundleName,
      List<BundleContainerRef> containerRefs,
      long size,
      PartialSegmentMetadataCacheEntry metadataEntry,
      List<PartialSegmentBundleCacheEntryIdentifier> parentEntryIds
  )
  {
    this.segmentId = segmentId;
    this.bundleName = bundleName;
    this.id = new PartialSegmentBundleCacheEntryIdentifier(segmentId, bundleName);
    this.containerRefs = containerRefs;
    this.size = size;
    this.metadataEntry = metadataEntry;
    this.parentEntryIds = parentEntryIds;
  }

  @Override
  public PartialSegmentBundleCacheEntryIdentifier getId()
  {
    return id;
  }

  @Override
  public long getSize()
  {
    return size;
  }

  @Override
  public boolean isMounted()
  {
    entryLock.lock();
    try {
      return mounted;
    }
    finally {
      entryLock.unlock();
    }
  }

  public SegmentId getSegmentId()
  {
    return segmentId;
  }

  public String getBundleName()
  {
    return bundleName;
  }

  /**
   * The list of {@link BundleContainerRef} this bundle owns, across the main file and any external files.
   */
  public List<BundleContainerRef> getContainerRefs()
  {
    return containerRefs;
  }

  public List<PartialSegmentBundleCacheEntryIdentifier> getParentEntryIds()
  {
    return parentEntryIds;
  }

  /**
   * Mount this bundle entry: acquire holds on the metadata entry and all transitive parent bundle entries, then
   * sparse-allocate every container this bundle owns. Concurrent calls are deduplicated via the {@link #mountFuture}
   * CAS gate, only one thread runs the work; the rest wait on the same future.
   * <p>
   * On failure, any holds taken are released and the gate is cleared so a subsequent retry gets a fresh attempt.
   */
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
        // clear the gate so the next caller gets a fresh attempt
        mountFuture.set(null);
        ours.setException(t);
        switch (t) {
          case IOException ioException -> throw ioException;
          case RuntimeException runtimeException -> throw runtimeException;
          case Error error -> throw error;
          default -> throw DruidException.defensive(t, "Failed to mount bundle entry[%s]", id);
        }
      }
      verifyStillReservedOrRollback(mountLocation);
      return;
    }
  }

  /**
   * Post-mount safety check: confirm the entry is still registered with the location, otherwise roll back. Handles
   * the race where a concurrent canceler releases the hold that was keeping this weak entry in {@code
   * weakCacheEntries} and the cache evicts it while mount() is still working. Without this check, mount would commit
   * local state (sparse-allocated containers on disk, parent holds + references) for an entry the cache manager no
   * longer knows about, leaking those resources. Mirrors the same defensive check in {@code SegmentCacheEntry.mount}.
   * Returns normally if rollback fires; callers detect via {@link #isMounted}.
   */
  private void verifyStillReservedOrRollback(StorageLocation mountLocation)
  {
    if (!mountLocation.isReserved(id) && !mountLocation.isWeakReserved(id)) {
      LOG.debug(
          "Aborting mount of bundle[%s] in location[%s]; entry was evicted while mounting",
          id,
          mountLocation.getPath()
      );
      unmount();
    }
  }

  private void doMount(StorageLocation mountLocation) throws IOException
  {
    // Pre-check inside entryLock; after this we release entryLock so the hold-acquisition + container-init work below
    // doesn't nest location.readLock under entryLock; same lock-order rule as the metadata entry's mount, which is
    // the inverse of StorageLocation.release's writeLock -> entryLock. The CAS+SettableFuture gate in mount()
    // guarantees only one thread runs this method at a time per entry, so we don't need entryLock to keep two
    // concurrent mounters out.
    entryLock.lock();
    try {
      if (mounted) {
        if (location != null && !location.equals(mountLocation)) {
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

    final PartialSegmentFileMapperV10 fileMapper = metadataEntry.getFileMapper();
    if (fileMapper == null) {
      throw DruidException.defensive(
          "Cannot mount bundle[%s]: metadata entry[%s] is not mounted",
          id,
          metadataEntry.getId()
      );
    }

    final List<StorageLocation.ReservationHold<?>> acquired = new ArrayList<>();
    final List<Closeable> acquiredRefs = new ArrayList<>();
    boolean registered = false;
    boolean committed = false;
    try {
      // 1. Cache holds on metadata + parents (prevents cache eviction of weak dependencies)
      final StorageLocation.ReservationHold<?> metadataHold =
          mountLocation.addWeakReservationHoldIfExists(metadataEntry.getId());
      if (metadataHold == null) {
        throw DruidException.defensive(
            "Cannot acquire metadata hold for [%s]; metadata entry not registered with location[%s]",
            metadataEntry.getId(),
            mountLocation.getPath()
        );
      }
      acquired.add(metadataHold);

      for (PartialSegmentBundleCacheEntryIdentifier parentId : parentEntryIds) {
        final StorageLocation.ReservationHold<?> parentHold =
            mountLocation.addWeakReservationHoldIfExists(parentId);
        if (parentHold == null) {
          throw DruidException.defensive(
              "Cannot acquire parent hold for [%s]; parent entry not registered with location[%s]",
              parentId,
              mountLocation.getPath()
          );
        }
        acquired.add(parentHold);
      }

      // 2. References on metadata + parents (gates their deferred cleanup on this bundle's lifetime; matters for
      // statically-reserved dependencies where a drop fires `release()` directly without going through cache)
      acquiredRefs.add(metadataEntry.acquireMetadataReference());
      for (PartialSegmentBundleCacheEntryIdentifier parentId : parentEntryIds) {
        final CacheEntry parentEntry = mountLocation.getCacheEntry(parentId);
        if (!(parentEntry instanceof PartialSegmentBundleCacheEntry)) {
          throw DruidException.defensive(
              "Parent entry[%s] of bundle[%s] is missing or not a bundle entry; cannot acquire reference",
              parentId,
              id
          );
        }
        acquiredRefs.add(((PartialSegmentBundleCacheEntry) parentEntry).acquireReference());
      }

      // 3. Sparse-allocate this bundle's containers, routing to the main mapper or the appropriate external mapper
      // depending on each ref's externalFilename.
      for (BundleContainerRef ref : containerRefs) {
        fileMapper.mapperForContainer(ref.externalFilename()).initializeContainer(ref.containerIndex());
      }

      // Register with metadata BEFORE the state commit. If this throws (it shouldn't, but just in case), no state has
      // been committed yet and the catch path releases the holds without leaving an orphaned-but-mounted bundle
      metadataEntry.registerBundle(this);
      registered = true;

      // Commit state under entryLock. Hold and reference ownership transfers from local lists to fields here. Also
      // install (or re-install, after a prior mount/unmount cycle terminated the previous Phaser) the reference-
      // counted gate over cleanup; future acquireReference() and unmount() calls operate on this instance.
      entryLock.lock();
      try {
        location = mountLocation;
        holds.addAll(acquired);
        dependencyReferences.addAll(acquiredRefs);
        mounted = true;
        references.set(new ReferenceCountingCloseableObject<Closeable>(this::doActualUnmount) {});
      }
      finally {
        entryLock.unlock();
      }
      committed = true;
    }
    finally {
      if (!committed) {
        // Evict any containers that were successfully initialized before the failure. Mirrors the eager
        // SegmentCacheEntry behavior: retry from a clean slate is simpler than reasoning about partial on-disk state.
        // evictContainer is a no-op for containers that were never initialized, so we can iterate the full set
        // without tracking how far the initialization loop got.
        for (BundleContainerRef ref : containerRefs) {
          try {
            fileMapper.mapperForContainer(ref.externalFilename()).evictContainer(ref.containerIndex());
          }
          catch (Throwable t) {
            LOG.warn(t, "Failed to evict container[%s/%d] for bundle[%s] during mount rollback",
                     ref.externalFilename(), ref.containerIndex(), id);
          }
        }
        if (registered) {
          try {
            metadataEntry.unregisterBundle(this);
          }
          catch (Throwable t) {
            LOG.warn(t, "Failed to unregister bundle[%s] during mount rollback", id);
          }
        }
        for (Closeable ref : acquiredRefs) {
          try {
            ref.close();
          }
          catch (Throwable t) {
            LOG.warn(t, "Failed to release dependency reference during mount rollback for bundle[%s]", id);
          }
        }
        for (StorageLocation.ReservationHold<?> hold : acquired) {
          try {
            hold.close();
          }
          catch (Throwable t) {
            LOG.warn(t, "Failed to release hold[%s] during mount rollback", hold);
          }
        }
      }
    }
  }

  /**
   * Triggers cleanup of this bundle. If any references acquired via {@link #acquireReference()} are still outstanding,
   * the actual evict/release work is deferred until the last reference releases; in that case this method returns
   * immediately and {@link #doActualUnmount} will fire later on the thread that closes the last reference. With no
   * outstanding references, cleanup runs synchronously on the caller's thread.
   */
  @Override
  public void unmount()
  {
    final ReferenceCountingCloseableObject<Closeable> current = references.get();
    if (current != null && !current.isClosed()) {
      current.close();
    }
  }

  /**
   * Acquire a reference that keeps this bundle's resources (container files, parent bundle holds) alive across an
   * intervening {@link #unmount} call. The returned {@link Closeable} must be closed when the caller is done; at
   * that point if {@code unmount()} has already been called and no other references remain, the deferred cleanup
   * fires on the closing thread.
   *
   * @throws DruidException if the bundle has never been mounted, or has already been cleaned up
   */
  public Closeable acquireReference()
  {
    final ReferenceCountingCloseableObject<Closeable> current = references.get();
    if (current == null) {
      throw DruidException.defensive(
          "Cannot acquire reference on bundle[%s] before it has been mounted",
          id
      );
    }
    return current.incrementReferenceAndDecrementOnceCloseable()
                  .orElseThrow(() -> DruidException.defensive(
                      "Cannot acquire reference on bundle[%s]; already being unmounted",
                      id
                  ));
  }

  /**
   * Current outstanding reference count (excluding the internal party released by {@link #unmount}). Returns 0 when
   * the bundle has never been mounted or has already been fully cleaned up.
   */
  public int getNumReferences()
  {
    final ReferenceCountingCloseableObject<Closeable> current = references.get();
    return (current == null || current.isClosed()) ? 0 : current.getNumReferences();
  }

  /**
   * The actual unmount work, invoked by the reference-counted gate's {@code onAdvance} once every outstanding
   * reference (plus the wrapper's own initial party) has been released. Evicts owned containers, releases parent
   * holds + dependency references, unregisters from the metadata entry, and clears the mount-dedup gate so a fresh
   * mount can run.
   * <p>
   * Dependency reference + cache hold releases happen OUTSIDE entryLock so that any cascading parent cleanup (a
   * parent whose last reference is this bundle's, draining the parent's Phaser) doesn't run under our lock and keeps
   * the entry-lock-then-location-lock convention intact even when the cascade re-enters StorageLocation.
   */
  private void doActualUnmount()
  {
    final List<Closeable> refsToRelease;
    final List<StorageLocation.ReservationHold<?>> holdsToRelease;
    entryLock.lock();
    try {
      if (!mounted) {
        return;
      }
      final PartialSegmentFileMapperV10 fileMapper = metadataEntry.getFileMapper();
      // file mapper may be null if metadata was already unmounted (out-of-order shutdown); evictContainer would NPE
      if (fileMapper != null) {
        for (BundleContainerRef ref : containerRefs) {
          try {
            fileMapper.mapperForContainer(ref.externalFilename()).evictContainer(ref.containerIndex());
          }
          catch (Throwable t) {
            LOG.warn(t, "Failed to evict container[%s/%d] for bundle[%s]", ref.externalFilename(), ref.containerIndex(), id);
          }
        }
      }
      refsToRelease = new ArrayList<>(dependencyReferences);
      dependencyReferences.clear();
      holdsToRelease = new ArrayList<>(holds);
      holds.clear();
      location = null;
      mounted = false;
      mountFuture.set(null);
    }
    finally {
      entryLock.unlock();
    }

    // Release dependency references first so any cascading parent cleanup runs before we drop cache holds. The order
    // is mostly informational since the two layers are independent, but matches the acquisition order in doMount.
    for (Closeable ref : refsToRelease) {
      try {
        ref.close();
      }
      catch (Throwable t) {
        LOG.warn(t, "Failed to release dependency reference for bundle[%s]", id);
      }
    }
    releaseHolds(holdsToRelease);
    metadataEntry.unregisterBundle(this);
  }

  /**
   * The distinct set of bundle names present across the segment's main file and every attached external file. A
   * container written without an explicit {@link SegmentFileBuilder#startFileBundle} call (or from a segment that
   * predates the bundle field) reports {@link SegmentFileBuilder#ROOT_BUNDLE_NAME}. Shared by the bootstrap
   * discovery path and {@link #resolveBundleName}.
   */
  public static Set<String> bundleNames(PartialSegmentFileMapperV10 fileMapper)
  {
    // Union of each mapper's own bundle names (each computed once at mapper construction).
    final Set<String> names = new HashSet<>(fileMapper.getBundleNames());
    for (String externalFilename : fileMapper.getExternalFilenames()) {
      names.addAll(fileMapper.getExternalMapper(externalFilename).getBundleNames());
    }
    return names;
  }

  /**
   * Resolve the bundle name to actually acquire for a query that asked for {@code requestedBundleName}. Normally the
   * requested name itself, but when no container exists with that name AND the segment's <em>only</em> bundle is
   * {@link SegmentFileBuilder#ROOT_BUNDLE_NAME}, this method resolves to the root bundle so the whole segment becomes
   * a single catch-all bundle and the query does not fail. This is to handle legacy v10 segments that might exist from
   * before the bundle name was stored in the metadata.
   * <p>
   * The fallback is deliberately gated on root being the segment's sole bundle:
   * <ul>
   *   <li>A segment with named bundles present never silently reroutes a missing request to root, that stays a hard
   *       error (a genuinely missing bundle is a bug).</li>
   *   <li>A future writer that legitimately writes some files to the root bundle <em>alongside</em> named bundles is
   *       unaffected: its reader asks for {@code __root__} by name, which exists, so the fallback never fires.</li>
   * </ul>
   */
  public static String resolveBundleName(PartialSegmentFileMapperV10 fileMapper, String requestedBundleName)
  {
    if (!findContainersForBundle(fileMapper, requestedBundleName).isEmpty()) {
      return requestedBundleName;
    }
    final Set<String> present = bundleNames(fileMapper);
    // legacy v10 segments from before bundle field was persisted in the segment
    if (present.size() == 1 && present.contains(SegmentFileBuilder.ROOT_BUNDLE_NAME)) {
      return SegmentFileBuilder.ROOT_BUNDLE_NAME;
    }
    // Requested bundle is absent and there are named bundles present: leave the name as-is so the caller fails
    // loudly (forBundle throws "no containers") rather than silently serving the wrong data.
    return requestedBundleName;
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

  private static void releaseHolds(Collection<StorageLocation.ReservationHold<?>> holds)
  {
    for (StorageLocation.ReservationHold<?> hold : holds) {
      try {
        hold.close();
      }
      catch (Throwable t) {
        LOG.warn(t, "Failed to release hold[%s]", hold);
      }
    }
  }

  /**
   * Reference to a single container that this bundle owns. {@code externalFilename} is {@code null} when the
   * container lives in the main V10 file, or the external file's name when the container lives in an attached
   * external file. {@code containerIndex} is the position within that file's
   * {@link SegmentFileMetadata#getContainers()} list. A single logical bundle (one named group) can span containers
   * across the main file and one or more external files when the writer propagates {@code startFileBundle} to both,
   * the cache layer treats them as one mount/evict unit regardless.
   */
  public record BundleContainerRef(@Nullable String externalFilename, int containerIndex)
  {
  }
}
