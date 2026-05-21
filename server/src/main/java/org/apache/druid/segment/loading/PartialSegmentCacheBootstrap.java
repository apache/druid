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
import org.apache.druid.segment.file.SegmentFileBuilder;
import org.apache.druid.segment.file.SegmentFileContainerMetadata;
import org.apache.druid.segment.file.SegmentFileMetadata;
import org.apache.druid.segment.projections.Projections;
import org.apache.druid.timeline.SegmentId;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Bootstraps partial-segment cache entries from existing on-disk state. Called by the cache manager on historical
 * startup for each segment directory that contains the partial-download layout (`{targetFilename}.header` plus one
 * or more `{targetFilename}.container.NNNNN` files).
 * <p>
 * The bootstrap is read-only with respect to deep storage; it never issues a range read. The on-disk header file is
 * parsed in-place by {@link PartialSegmentFileMapperV10#create} (which detects header corruption and, for that one
 * case, may delete the local copy; bootstrap callers should treat that as "no restorable state" and fall back to a
 * cold start). Bundle entries are registered as weak entries on the storage location, mounted (which sparse-allocates
 * any container files that weren't already present and re-establishes parent holds), and returned to the caller.
 * <p>
 * Parent-set inference is delegated to {@link PartialSegmentMetadataCacheEntry#inferParentBundles}. A bundle whose
 * inferred parent isn't itself present on disk is treated as <i>orphaned</i>: its on-disk container files are deleted
 * (via {@link PartialSegmentFileMapperV10#evictContainer}, which also clears the relevant bitmap bits) and the bundle
 * is not restored. The next access through the cache manager acquire path then triggers a clean cold re-fetch, the
 * same fall-back as when the cache manager finds a segment listed in the info directory but missing on disk.
 */
public final class PartialSegmentCacheBootstrap
{
  private static final EmittingLogger LOG = new EmittingLogger(PartialSegmentCacheBootstrap.class);

  /**
   * Restore a single partial segment's cache entries from its local on-disk layout.
   *
   * @param segmentId         the segment whose entries are being restored
   * @param localCacheDir     the per-segment directory containing the header + container files
   * @param targetFilename    the V10 entry-point filename
   * @param externalFilenames any external segment file names that were registered as children of the entry-point
   *                          file
   * @param jsonMapper        used to parse the header
   * @param location          the storage location these entries belong to; the metadata entry is registered as
   *                          static and bundle entries are registered as weak
   * @throws IllegalStateException if the expected header file is missing or unreadable
   * @throws IOException           propagated from {@link CacheEntry#mount} if mount fails
   */
  public static RestoreResult restoreFromDisk(
      SegmentId segmentId,
      File localCacheDir,
      String targetFilename,
      List<String> externalFilenames,
      ObjectMapper jsonMapper,
      StorageLocation location
  ) throws IOException
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
        BootstrapRangeReader.INSTANCE,
        jsonMapper,
        actualMetadataSize
    );

    if (!location.reserve(metadata)) {
      throw DruidException.defensive(
          "Failed to reserve metadata entry for partial segment[%s] at location[%s]",
          segmentId,
          location.getPath()
      );
    }

    // From here, any throw must roll back: unmount any successfully-mounted bundles (releases their references on
    // metadata) and then release the metadata entry from the location. Without this, a mid-bootstrap failure would
    // leave the location partially reserved + mounted, which would confuse later restores and leak disk/memory.
    final List<PartialSegmentBundleCacheEntry> mountedBundles = new ArrayList<>();
    boolean success = false;
    try {
      metadata.mount(location);

      // Discover bundle names across the main file and every external file, then keep only those whose owned
      // container files actually exist on disk. Walks via the file mapper so the external mappers' SegmentFileMetadata
      // are visited too — bundles can legitimately span the main file and one or more externals when the writer
      // propagates startFileBundle across them.
      final PartialSegmentFileMapperV10 fileMapper = metadata.getFileMapper();
      if (fileMapper == null) {
        throw DruidException.defensive(
            "Metadata entry mount produced null file mapper for segment[%s]",
            segmentId
        );
      }
      final Set<String> candidateBundleNames = discoverBundleNames(fileMapper);
      final List<String> presentBundleNames = filterByContainerPresence(
          candidateBundleNames,
          fileMapper,
          localCacheDir
      );

      // Classify each present bundle as either mountable or orphaned. A bundle is orphaned when its inferred parent
      // set includes a bundle that isn't itself present on disk; restoring it would only produce a degenerate state
      // where column reads that resolve into the missing parent would fail at query time. Instead, delete the
      // orphan's on-disk containers so the next access triggers a clean cold re-fetch from deep storage
      final List<String> mountableBundleNames = new ArrayList<>();
      final Set<String> orphanedBundleNames = new HashSet<>();
      for (String name : presentBundleNames) {
        boolean orphaned = false;
        for (PartialSegmentBundleCacheEntryIdentifier parent : metadata.inferParentBundles(name)) {
          if (!presentBundleNames.contains(parent.bundleName())) {
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
            "Deleted on-disk state of orphaned bundle[%s] for segment[%s] (parent unrestorable); next access "
            + "will trigger cold re-fetch",
            orphanName,
            segmentId
        );
      }

      // mount base bundle before any dependent bundle so its hold is available when dependents acquire parent holds
      mountableBundleNames.sort(Comparator.comparing(name -> !Projections.BASE_TABLE_PROJECTION_NAME.equals(name)));

      for (String bundleName : mountableBundleNames) {
        // Mountable bundles have all parents present by construction (orphans were filtered out above), so the
        // inferred parent set is exactly what we want, no further filtering needed.
        final List<PartialSegmentBundleCacheEntryIdentifier> parentIds = metadata.inferParentBundles(bundleName);
        final PartialSegmentBundleCacheEntry bundle = PartialSegmentBundleCacheEntry.forBundle(
            metadata,
            bundleName,
            parentIds
        );
        // weak-reserve with a temporary hold so the mount call's own parent-hold acquisition can succeed; release the
        // bootstrap hold immediately after, if the entry should remain alive for query-side access, the runtime hold
        // chain (transitive parents from aggregates, segment-level holds from acquire APIs) keeps it pinned.
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

      LOG.debug(
          "Restored partial segment[%s] from [%s]: metadata size[%d], bundles[%s], orphans[%s]",
          segmentId,
          localCacheDir,
          actualMetadataSize,
          mountableBundleNames,
          orphanedBundleNames
      );
      success = true;
      return new RestoreResult(metadata, mountedBundles);
    }
    finally {
      if (!success) {
        // Roll back in reverse-dependency order: bundles first (so they release references on metadata + parents)
        // then the metadata entry itself. The bundle/metadata cleanup is best-effort, log and continue rather than
        // shadow the original failure.
        for (PartialSegmentBundleCacheEntry bundle : mountedBundles) {
          try {
            bundle.unmount();
          }
          catch (Throwable t) {
            LOG.warn(t, "Failed to roll back bundle[%s] during bootstrap failure for [%s]", bundle.getId(), segmentId);
          }
        }
        try {
          location.release(metadata);
        }
        catch (Throwable t) {
          LOG.warn(t, "Failed to roll back metadata entry for partial segment[%s]", segmentId);
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
   * Discover the bundle names present in a segment by walking each container's
   * {@link SegmentFileContainerMetadata#getBundle bundle} across the main file and every attached external file.
   * The bundle field is always non-null — containers written without an explicit {@code startFileBundle} call
   * (including those from older segments) default to {@link SegmentFileBuilder#ROOT_BUNDLE_NAME}.
   */
  private static Set<String> discoverBundleNames(PartialSegmentFileMapperV10 fileMapper)
  {
    final Set<String> names = new HashSet<>();
    collectBundleNames(fileMapper.getSegmentFileMetadata(), names);
    for (String externalFilename : fileMapper.getExternalFilenames()) {
      collectBundleNames(fileMapper.getExternalMapper(externalFilename).getSegmentFileMetadata(), names);
    }
    return names;
  }

  private static void collectBundleNames(SegmentFileMetadata fileMeta, Set<String> out)
  {
    for (SegmentFileContainerMetadata container : fileMeta.getContainers()) {
      out.add(container.getBundle());
    }
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

  /**
   * Stub range reader used during bootstrap: the on-disk header is expected to exist and parse, so no fetch is needed.
   * If for any reason {@link PartialSegmentFileMapperV10#create} decides to re-fetch (e.g. header corruption), this
   * reader throws so we fail loudly rather than silently re-downloading without the operator's knowledge.
   */
  private static final class BootstrapRangeReader implements SegmentRangeReader
  {
    static final BootstrapRangeReader INSTANCE = new BootstrapRangeReader();

    @Override
    public InputStream readRange(String filename, long offset, long length)
    {
      throw DruidException.defensive(
          "BootstrapRangeReader was asked to fetch [%s] @[%d:%d]; bootstrap should only read from local disk",
          Objects.toString(filename),
          offset,
          length
      );
    }
  }

  /**
   * Hold-acquire result of a partial-segment restore: the always-static metadata entry plus the list of bundle
   * entries (already mounted, registered as weak entries) discovered on disk for this segment.
   */
  public static final class RestoreResult
  {
    private final PartialSegmentMetadataCacheEntry metadata;
    private final List<PartialSegmentBundleCacheEntry> bundles;

    RestoreResult(PartialSegmentMetadataCacheEntry metadata, List<PartialSegmentBundleCacheEntry> bundles)
    {
      this.metadata = metadata;
      this.bundles = List.copyOf(bundles);
    }

    public PartialSegmentMetadataCacheEntry getMetadata()
    {
      return metadata;
    }

    public List<PartialSegmentBundleCacheEntry> getBundles()
    {
      return bundles;
    }
  }
}
