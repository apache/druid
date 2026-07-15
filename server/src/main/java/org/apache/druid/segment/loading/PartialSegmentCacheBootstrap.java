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
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.file.PartialSegmentFileMapperV10;
import org.apache.druid.segment.projections.Projections;
import org.apache.druid.timeline.SegmentId;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Bootstraps partial-segment cache entries from existing on-disk state. Called by the cache manager on historical
 * startup for each segment directory that contains the partial-download layout (`{targetFilename}.header` plus one
 * or more `{targetFilename}.container.NNNNN` files).
 * <p>
 * The bootstrap is read-only with respect to deep storage; it never issues a range read. The on-disk header file is
 * parsed in-place by {@link PartialSegmentFileMapperV10#create} (which detects header corruption and, for that one
 * case, may delete the local copy; bootstrap callers should treat that as "no restorable state" and fall back to a
 * cold start).
 * <p>
 * Two-phase contract:
 * <ol>
 *   <li>{@link #reserveFromDisk} (called from {@code getCachedSegments}, light): validates the header file is present,
 *   computes the actual on-disk reservation size, constructs the metadata cache entry, reserves it on the location.
 *   No mount, no range read.</li>
 *   <li>{@code metadata.mount(location)} (called from {@code bootstrap()} via the bootstrap executor, parallelizable):
 *   the metadata entry's own mount path builds the file mapper from the on-disk header, then internally invokes
 *   {@link #restoreBundlesFromDisk} to discover, reserve, and mount any bundles whose container files survived. The
 *   same call from the fresh acquire path is a no-op (no on-disk containers to restore).</li>
 * </ol>
 * Dependency inference is delegated to {@link PartialSegmentMetadataCacheEntry#inferBundleDependencies}. A bundle
 * whose inferred dependency isn't itself present on disk is treated as <i>orphaned</i>: its on-disk container files
 * are deleted (via {@link PartialSegmentFileMapperV10#evictContainer}, which also clears the relevant bitmap bits) and
 * the bundle is not restored. The next access through the cache manager acquire path then triggers a clean cold
 * re-fetch, the same fall-back as when the cache manager finds a segment listed in the info directory but missing on
 * disk.
 */
public final class PartialSegmentCacheBootstrap
{
  private static final EmittingLogger LOG = new EmittingLogger(PartialSegmentCacheBootstrap.class);

  /**
   * Reserve a partial segment's metadata cache entry on the supplied location from the on-disk header. Light-weight:
   * no range read, no file mapper, no bundle work. The entry is registered as a weak cache entry (consistent with
   * the runtime acquire path), so it is evictable once mounted; bootstrap-restored data is treated as a cache
   * optimization, not a permanent fixture. The caller is expected to drive the actual mount through
   * {@code metadata.mount(location)} later (typically via {@code SegmentCacheManager#bootstrap}), which builds the
   * file mapper (parsing the header from local disk, no fetch) and cascades into {@link #restoreBundlesFromDisk}.
   *
   * @param segmentId         the segment whose entries are being restored
   * @param localCacheDir     the per-segment directory containing the header + container files
   * @param targetFilename    the V10 entry-point filename
   * @param externalFilenames any external segment file names that were registered as children of the entry-point file
   * @param rangeReader       the segment's deep-storage range reader, retained for later on-demand fetches
   * @param jsonMapper        used by the metadata entry's mount path to parse the header
   * @param storagePool       thread pool the async cursor path submits on-demand column downloads to (which bounds
   *                          load concurrency itself); may be null in tests that never invoke the cursor factory
   * @param location          the storage location to reserve the metadata entry on
   * @param coalesceGapBytes  gap tolerance for coalesced range downloads, see
   *                          {@link PartialSegmentFileMapperV10#fetchFiles}
   * @param maxFetchRunBytes  size cap for parallel-fetch range reads, see
   *                          {@link PartialSegmentFileMapperV10#planParallelFetch}
   * @return the reserved {@link PartialSegmentMetadataCacheEntry}; the caller is responsible for mounting it
   * @throws DruidException if the expected header file is missing or the location cannot accept the reservation
   */
  public static PartialSegmentMetadataCacheEntry reserveFromDisk(
      SegmentId segmentId,
      File localCacheDir,
      String targetFilename,
      List<String> externalFilenames,
      SegmentRangeReader rangeReader,
      ObjectMapper jsonMapper,
      @Nullable StorageLoadingThreadPool storagePool,
      StorageLocation location,
      long coalesceGapBytes,
      long maxFetchRunBytes
  )
  {
    final File headerFile = new File(localCacheDir, targetFilename + PartialSegmentFileMapperV10.METADATA_HEADER_SUFFIX);
    if (!headerFile.exists()) {
      throw DruidException.defensive(
          "No on-disk header for partial segment[%s] at [%s]; nothing to restore",
          segmentId,
          headerFile
      );
    }

    // size the metadata reservation to the actual on-disk size so the location accounting is correct from the start
    final long actualMetadataSize = computeOnDiskHeaderSize(localCacheDir, targetFilename, externalFilenames);
    final PartialSegmentMetadataCacheEntry metadata = new PartialSegmentMetadataCacheEntry(
        segmentId,
        localCacheDir,
        targetFilename,
        externalFilenames,
        rangeReader,
        jsonMapper,
        storagePool,
        actualMetadataSize,
        coalesceGapBytes,
        maxFetchRunBytes
    );

    if (!location.reserveWeak(metadata)) {
      throw DruidException.defensive(
          "Failed to reserve metadata entry for partial segment[%s] at location[%s]",
          segmentId,
          location.getPath()
      );
    }
    return metadata;
  }

  /**
   * Discover, reserve, and mount any bundles whose container files survived on disk for the given partial segment.
   * Invoked from {@link PartialSegmentMetadataCacheEntry#mount} after the file mapper has been built; safe to call
   * unconditionally (on the fresh-acquire path there are no on-disk containers yet, so the call is a no-op).
   * <p>
   * On any failure during bundle reservation or mount, attempts to roll back any partially-mounted bundles before
   * propagating the throw. The metadata entry itself is NOT released here, the caller (typically
   * {@link PartialSegmentMetadataCacheEntry#doMount}) handles metadata-level rollback on a propagated throw.
   */
  static void restoreBundlesFromDisk(PartialSegmentMetadataCacheEntry metadata, StorageLocation location)
      throws IOException
  {
    final PartialSegmentFileMapperV10 fileMapper = metadata.getFileMapper();
    if (fileMapper == null) {
      // metadata isn't mounted yet (or has already been unmounted); nothing to restore
      return;
    }

    final SegmentId segmentId = metadata.getSegmentId();
    final File localCacheDir = metadata.getLocalCacheDir();

    // Discover bundle names across the main file and every external file, then keep only those whose owned
    // container files actually exist on disk. Walks via the file mapper so the external mappers' SegmentFileMetadata
    // are visited too; bundles can legitimately span the main file and one or more externals when the writer
    // propagates startFileBundle across them.
    final Set<String> candidateBundleNames = PartialSegmentBundleCacheEntry.bundleNames(fileMapper);
    final List<String> presentBundleNames = filterByContainerPresence(
        candidateBundleNames,
        fileMapper,
        localCacheDir
    );
    if (presentBundleNames.isEmpty()) {
      // Fresh acquire path or a partial whose containers were all evicted: nothing to do.
      return;
    }

    // Classify each present bundle as either mountable or orphaned. A bundle is orphaned when its inferred parent
    // set includes a bundle that isn't itself present on disk; restoring it would only produce a degenerate state
    // where column reads that resolve into the missing parent would fail at query time. Instead, delete the
    // orphan's on-disk containers so the next access triggers a clean cold re-fetch from deep storage.
    final List<String> mountableBundleNames = new ArrayList<>();
    final Set<String> orphanedBundleNames = new HashSet<>();
    for (String name : presentBundleNames) {
      boolean orphaned = false;
      for (PartialSegmentBundleCacheEntryIdentifier dep : metadata.inferBundleDependencies(name)) {
        if (!presentBundleNames.contains(dep.bundleName())) {
          orphaned = true;
          break;
        }
      }
      if (orphaned) {
        orphanedBundleNames.add(name);
      } else {
        mountableBundleNames.add(name);
      }
    }

    for (String orphanName : orphanedBundleNames) {
      for (PartialSegmentBundleCacheEntry.BundleContainerRef ref :
          PartialSegmentBundleCacheEntry.findContainersForBundle(fileMapper, orphanName)) {
        fileMapper.mapperForContainer(ref.externalFilename()).evictContainer(ref.containerIndex());
      }
      LOG.debug(
          "Deleted on-disk state of orphaned bundle[%s] for segment[%s] (dependency unrestorable); next access "
          + "will trigger cold re-fetch",
          orphanName,
          segmentId
      );
    }

    // Mount the base bundle before any dependent bundle so its hold is available when dependents acquire deps.
    mountableBundleNames.sort(Comparator.comparing(name -> !Projections.BASE_TABLE_PROJECTION_NAME.equals(name)));

    final List<PartialSegmentBundleCacheEntry> mountedBundles = new ArrayList<>();
    boolean success = false;
    try {
      for (String bundleName : mountableBundleNames) {
        // Mountable bundles have all dependencies present by construction (orphans were filtered out above), so the
        // inferred dependency set is exactly what we want, no further filtering needed.
        final List<PartialSegmentBundleCacheEntryIdentifier> parentIds = metadata.inferBundleDependencies(bundleName);
        final PartialSegmentBundleCacheEntry bundle = PartialSegmentBundleCacheEntry.forBundle(
            metadata,
            bundleName,
            parentIds
        );
        // weak-reserve with a temporary hold so the mount call's own parent-hold acquisition can succeed; release the
        // bootstrap hold immediately after, if the entry should remain alive for query-side access, the runtime
        // hold chain (transitive parents from aggregates, segment-level holds from acquire APIs) keeps it pinned.
        try (StorageLocation.ReservationHold<?> bootstrapHold =
                 location.addWeakReservationHold(bundle.getId(), () -> bundle)) {
          if (bootstrapHold == null) {
            throw DruidException.defensive(
                "Failed to reserve bundle entry[%s] in location[%s] during bootstrap",
                bundle.getId(),
                location.getPath()
            );
          }
          bundle.mount(location);
        }
        mountedBundles.add(bundle);
      }
      success = true;
      LOG.debug(
          "Restored bundles for partial segment[%s] from [%s]: bundles[%s], orphans[%s]",
          segmentId,
          localCacheDir,
          mountableBundleNames,
          orphanedBundleNames
      );
    }
    finally {
      if (!success) {
        // Reverse-dependency rollback for bundles only; the metadata entry's own rollback is the caller's
        // responsibility (it will fire when the propagated throw escapes doMount's try/catch).
        for (PartialSegmentBundleCacheEntry bundle : mountedBundles) {
          try {
            bundle.unmount();
          }
          catch (Throwable t) {
            LOG.warn(t, "Failed to roll back bundle[%s] during bootstrap failure for [%s]", bundle.getId(), segmentId);
          }
        }
      }
    }
  }

  /**
   * Check whether a directory looks like a partial-segment cache layout for the given target filename.
   */
  public static boolean isPartialSegmentLayout(File localCacheDir, String targetFilename)
  {
    if (localCacheDir == null || !localCacheDir.isDirectory()) {
      return false;
    }
    final File header = new File(
        localCacheDir,
        targetFilename + PartialSegmentFileMapperV10.METADATA_HEADER_SUFFIX
    );
    return header.exists();
  }

  private static long computeOnDiskHeaderSize(File localCacheDir, String targetFilename, List<String> externalFilenames)
  {
    long total = sizeOf(new File(
        localCacheDir,
        targetFilename + PartialSegmentFileMapperV10.METADATA_HEADER_SUFFIX
    ));
    for (String external : externalFilenames) {
      total += sizeOf(new File(
          localCacheDir,
          external + PartialSegmentFileMapperV10.METADATA_HEADER_SUFFIX
      ));
    }
    if (total <= 0) {
      // PartialSegmentMetadataCacheEntry requires a positive reservation; if all headers are zero-length the local
      // layout is degenerate and should not be restored
      throw DruidException.defensive(
          "Zero-sized header files in [%s]; refusing to restore",
          localCacheDir
      );
    }
    return total;
  }

  private static long sizeOf(File f)
  {
    return f.exists() ? f.length() : 0;
  }

  /**
   * Keep only bundles whose every owned container file exists on disk. The on-disk path for a container is
   * {@code {mapperTargetFilename}.container.{containerIndex:05d}} where {@code mapperTargetFilename} is the main
   * V10 filename for refs in the main mapper, or the external filename for refs in an external mapper.
   */
  private static List<String> filterByContainerPresence(
      Set<String> candidateBundleNames,
      PartialSegmentFileMapperV10 fileMapper,
      File localCacheDir
  )
  {
    final List<String> restorable = new ArrayList<>();
    for (String bundleName : candidateBundleNames) {
      final List<PartialSegmentBundleCacheEntry.BundleContainerRef> refs =
          PartialSegmentBundleCacheEntry.findContainersForBundle(fileMapper, bundleName);
      if (refs.isEmpty()) {
        continue;
      }
      boolean allPresent = true;
      for (PartialSegmentBundleCacheEntry.BundleContainerRef ref : refs) {
        final String mapperFilename = fileMapper.mapperForContainer(ref.externalFilename()).getTargetFilename();
        final File cf = new File(
            localCacheDir,
            StringUtils.format("%s.container.%05d", mapperFilename, ref.containerIndex())
        );
        if (!cf.exists()) {
          allPresent = false;
          break;
        }
      }
      if (allPresent) {
        restorable.add(bundleName);
      }
    }
    return restorable;
  }

  private PartialSegmentCacheBootstrap()
  {
    // utility class
  }

}
