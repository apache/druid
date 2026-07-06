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

package org.apache.druid.segment.file;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Ints;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.ByteBufferUtils;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.CompressedPools;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.loading.SegmentRangeReader;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A {@link SegmentFileMapper} that downloads internal files on demand from deep storage via a
 * {@link SegmentRangeReader}. This enables partial segment downloads where only the files needed for a query are
 * fetched, rather than downloading the entire segment.
 * <p>
 * Locally, this mapper mirrors the original V10 container structure: each container from the segment file is
 * represented as a local sparse file at its original size, and only the byte ranges for downloaded internal files are
 * populated. This means the number of local files (and mmaps) equals the number of containers (typically 1-3 at
 * 'standard' segment sizes) rather than the number of internal files (which could be hundreds).
 * <p>
 * The {@link MappedByteBuffer} for each container is created once and then the channel is closed immediately (same
 * pattern as {@link SegmentFileMapperV10}). Writes of downloaded file data use short-lived {@link RandomAccessFile}
 * instances. The mmap reflects writes through the shared page cache.
 * <p>
 * State is persisted to disk so that the mapper can be restored after a process restart without re-fetching metadata
 * from deep storage. The raw V10 header bytes are written to a local file, and a compact bitmap file is appended to
 * the end of it to track which internal files have been downloaded (one bit per file, updated after each download). On
 * subsequent calls, the metadata is parsed from the local file instead of range-reading from deep storage.
 * <p>
 * External segment files are supported via child {@link PartialSegmentFileMapperV10} instances, each targeting a
 * different file in the segment's storage location.
 * <p>
 * Thread-safe for concurrent access from multiple queries. Per-file locks prevent duplicate downloads of the same
 * internal file.
 *
 * @see SegmentFileMapperV10
 * @see SegmentRangeReader
 */
public class PartialSegmentFileMapperV10 implements SegmentFileMapper
{
  private static final Logger LOG = new Logger(PartialSegmentFileMapperV10.class);

  /**
   * Suffix appended to the target filename to form the local header file. Public so cache-manager components can
   * recognize the partial-download on-disk layout during bootstrap restore and reservation cleanup.
   */
  public static final String METADATA_HEADER_SUFFIX = ".header";

  /**
   * Create (or restore) a lazy mapper for the main segment file with attached external file mappers. If persisted state
   * exists locally from a previous session, metadata is read from disk. Otherwise, metadata is fetched from deep
   * storage via range reads and persisted locally.
   */
  public static PartialSegmentFileMapperV10 create(
      SegmentRangeReader rangeReader,
      ObjectMapper jsonMapper,
      File localCacheDir,
      String targetFilename,
      List<String> externals,
      PartialSegmentDownloadListener downloadListener
  ) throws IOException
  {
    final PartialSegmentFileMapperV10 entryPoint = createForFile(
        rangeReader,
        jsonMapper,
        localCacheDir,
        targetFilename,
        downloadListener
    );

    final Map<String, PartialSegmentFileMapperV10> externalMappers = new HashMap<>();
    try {
      for (String filename : externals) {
        externalMappers.put(
            filename,
            createForFile(rangeReader, jsonMapper, localCacheDir, filename, downloadListener)
        );
      }
    }
    catch (Throwable t) {
      Closer closer = Closer.create();
      closer.register(entryPoint);
      closer.registerAll(externalMappers.values());
      throw CloseableUtils.closeAndWrapInCatch(t, closer);
    }

    entryPoint.externalMappers.putAll(externalMappers);
    return entryPoint;
  }

  @VisibleForTesting
  static PartialSegmentFileMapperV10 createForFile(
      SegmentRangeReader rangeReader,
      ObjectMapper jsonMapper,
      File localCacheDir,
      String targetFilename,
      PartialSegmentDownloadListener downloadListener
  ) throws IOException
  {
    FileUtils.mkdirp(localCacheDir);
    final File headerFile = new File(localCacheDir, targetFilename + METADATA_HEADER_SUFFIX);

    // try to load from existing local file, re-fetching from deep storage if missing or corrupted
    SegmentFileMetadataReader.Result result = null;
    MappedByteBuffer bitmapBuffer = null;

    if (headerFile.exists()) {
      try {
        result = parseHeaderFile(headerFile, jsonMapper);
        bitmapBuffer = mmapBitmap(headerFile, result);
      }
      catch (Exception e) {
        // corrupted file (partial write, truncated bitmap, bad JSON, etc.), delete and re-fetch
        result = null;
        if (!headerFile.delete()) {
          LOG.warn(
              e,
              "Failed to delete corrupted header file[%s] for [%s]; will be overwritten by re-fetch",
              headerFile,
              targetFilename
          );
        }
      }
    }

    if (result == null) {
      fetchAndPersistHeader(rangeReader, targetFilename, headerFile);
      result = parseHeaderFile(headerFile, jsonMapper);
      bitmapBuffer = mmapBitmap(headerFile, result);
      downloadListener.onBytesDownloaded(headerFile.length());
    }

    final PartialSegmentFileMapperV10 mapper = new PartialSegmentFileMapperV10(
        result.getMetadata(),
        result.getHeaderSize(),
        rangeReader,
        targetFilename,
        localCacheDir,
        bitmapBuffer,
        downloadListener
    );

    // bitmap-vs-container repair pre-pass: if the bitmap claims a file is downloaded but its container file is
    // missing on disk, the bitmap is lying (e.g. partial-cache eviction that cleared containers but couldn't atomically
    // clear bits, or external file-system damage). Clear those bits before the restore loop so we don't spuriously
    // sparse-allocate empty containers in the restore loop's ensureContainerInitialized call and treat their files as
    // downloaded.
    for (int i = 0; i < mapper.sortedFileNames.size(); i++) {
      final int byteIndex = i / 8;
      final int bitMask = 1 << (i % 8);
      if ((bitmapBuffer.get(byteIndex) & bitMask) == 0) {
        continue;
      }
      final String name = mapper.sortedFileNames.get(i);
      final SegmentInternalFileMetadata fileMetadata = result.getMetadata().getFiles().get(name);
      if (fileMetadata == null) {
        continue;
      }
      final File containerFile = new File(
          localCacheDir,
          StringUtils.format("%s.container.%05d", targetFilename, fileMetadata.getContainer())
      );
      if (!containerFile.exists()) {
        bitmapBuffer.put(byteIndex, (byte) (bitmapBuffer.get(byteIndex) & ~bitMask));
      }
    }

    // restore downloaded files from the (now-repaired) bitmap
    for (int i = 0; i < mapper.sortedFileNames.size(); i++) {
      final int byteIndex = i / 8;
      final int bitIndex = i % 8;
      if ((bitmapBuffer.get(byteIndex) & (1 << bitIndex)) != 0) {
        final String name = mapper.sortedFileNames.get(i);
        final SegmentInternalFileMetadata fileMetadata = result.getMetadata().getFiles().get(name);
        if (fileMetadata != null) {
          mapper.ensureContainerInitialized(fileMetadata.getContainer());
          mapper.downloadedFiles.add(name);
          mapper.downloadedBytes.addAndGet(fileMetadata.getSize());
        }
      }
    }

    return mapper;
  }

  private final SegmentFileMetadata metadata;
  private final long headerSize;
  private final SegmentRangeReader rangeReader;
  private final String targetFilename;
  private final File localCacheDir;

  // stable sorted ordering of file names for bitmap indexing
  private final List<String> sortedFileNames;
  private final Map<String, Integer> fileNameToIndex;

  // per-container state, lazily initialized
  private final MappedByteBuffer[] containers;
  private final File[] containerFiles;
  private final ReentrantLock[] containerLocks;

  // bundle name -> indices (into metadata.getContainers()) of this single mapper's containers in that bundle.
  // Computed once at construction from the immutable container metadata. Single-mapper scope only: stitching bundles
  // across attached external mappers is the cache layer's concern (PartialSegmentBundleCacheEntry).
  private final Map<String, List<Integer>> bundleToContainerIndices;

  // file names per container index (parallel to metadata.getContainers()), for whole-container bulk download. Built
  // once from the immutable metadata.
  private final List<List<String>> containerFileNames;

  // external file mappers
  private final Map<String, PartialSegmentFileMapperV10> externalMappers = new HashMap<>();

  // track which internal files have been downloaded
  private final Set<String> downloadedFiles = ConcurrentHashMap.newKeySet();
  private final ConcurrentHashMap<String, ReentrantLock> fileLocks = new ConcurrentHashMap<>();
  private final ReentrantLock bitmapLock;
  private final MappedByteBuffer bitmapBuffer;
  private final AtomicLong downloadedBytes = new AtomicLong(0);
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final PartialSegmentDownloadListener downloadListener;

  private PartialSegmentFileMapperV10(
      SegmentFileMetadata metadata,
      long headerSize,
      SegmentRangeReader rangeReader,
      String targetFilename,
      File localCacheDir,
      MappedByteBuffer bitmapBuffer,
      PartialSegmentDownloadListener downloadListener
  )
  {
    this.metadata = metadata;
    this.headerSize = headerSize;
    this.rangeReader = rangeReader;
    this.targetFilename = targetFilename;
    this.localCacheDir = localCacheDir;
    this.bitmapBuffer = bitmapBuffer;
    this.downloadListener = downloadListener;

    // build stable file name ordering for bitmap indexing
    this.sortedFileNames = new ArrayList<>(new TreeSet<>(metadata.getFiles().keySet()));
    this.fileNameToIndex = new HashMap<>();
    for (int i = 0; i < sortedFileNames.size(); i++) {
      fileNameToIndex.put(sortedFileNames.get(i), i);
    }

    final List<SegmentFileContainerMetadata> containerMetas = metadata.getContainers();
    final int numContainers = containerMetas.size();
    this.containers = new MappedByteBuffer[numContainers];
    this.containerFiles = new File[numContainers];
    this.containerLocks = new ReentrantLock[numContainers];
    final Map<String, List<Integer>> bundleIndices = new HashMap<>();
    for (int i = 0; i < numContainers; i++) {
      this.containerLocks[i] = new ReentrantLock();
      bundleIndices.computeIfAbsent(containerMetas.get(i).getBundle(), name -> new ArrayList<>()).add(i);
    }
    bundleIndices.replaceAll((name, indices) -> List.copyOf(indices));
    this.bundleToContainerIndices = Map.copyOf(bundleIndices);

    final List<List<String>> perContainer = new ArrayList<>(numContainers);
    for (int i = 0; i < numContainers; i++) {
      perContainer.add(new ArrayList<>());
    }
    for (Map.Entry<String, SegmentInternalFileMetadata> entry : metadata.getFiles().entrySet()) {
      perContainer.get(entry.getValue().getContainer()).add(entry.getKey());
    }
    perContainer.replaceAll(List::copyOf);
    this.containerFileNames = List.copyOf(perContainer);

    this.bitmapLock = new ReentrantLock();
  }

  public SegmentFileMetadata getSegmentFileMetadata()
  {
    return metadata;
  }

  /**
   * The distinct bundle names present in this single mapper's containers (not including any attached external
   * mappers). Computed once at construction.
   */
  public Set<String> getBundleNames()
  {
    return bundleToContainerIndices.keySet();
  }

  /**
   * Indices, into this mapper's {@link SegmentFileMetadata#getContainers()} list, of the containers belonging to
   * {@code bundleName}; empty if this mapper has no containers in that bundle.
   */
  public List<Integer> getContainerIndicesForBundle(String bundleName)
  {
    return bundleToContainerIndices.getOrDefault(bundleName, List.of());
  }

  /**
   * Names of the external segment files attached to this mapper (each one is its own {@link PartialSegmentFileMapperV10}
   * accessible via {@link #getExternalMapper}). Empty for mappers with no externals.
   */
  public Set<String> getExternalFilenames()
  {
    return externalMappers.keySet();
  }

  /**
   * Look up the child mapper for an external segment file. Returns {@code null} if no external with that name is
   * attached. Cache-layer callers use this to walk external files' {@link SegmentFileMetadata} and route
   * {@link #initializeContainer} / {@link #evictContainer} calls to the right physical file.
   */
  @Nullable
  public PartialSegmentFileMapperV10 getExternalMapper(String externalFilename)
  {
    return externalMappers.get(externalFilename);
  }

  /**
   * Resolve {@code this} when {@code externalFilename} is null (main file), otherwise the named external child
   * mapper. Throws if the external is not attached. Useful for routing container operations from cache-layer code
   * that holds {@code (externalFilename, containerIndex)} refs.
   */
  public PartialSegmentFileMapperV10 mapperForContainer(@Nullable String externalFilename)
  {
    if (externalFilename == null) {
      return this;
    }
    final PartialSegmentFileMapperV10 external = externalMappers.get(externalFilename);
    if (external == null) {
      throw DruidException.defensive(
          "External mapper[%s] is not attached to this mapper for [%s]",
          externalFilename,
          targetFilename
      );
    }
    return external;
  }

  /**
   * The {@code targetFilename} this mapper writes/reads to/from inside the cache directory. For the entry-point
   * mapper this is e.g. {@link org.apache.druid.segment.IndexIO#V10_FILE_NAME}; for an external child mapper it's
   * the external file's name.
   */
  public String getTargetFilename()
  {
    return targetFilename;
  }

  @Override
  public Set<String> getInternalFilenames()
  {
    return metadata.getFiles().keySet();
  }

  @Nullable
  @Override
  public ByteBuffer mapFile(String name) throws IOException
  {
    checkClosed();

    final SegmentInternalFileMetadata fileMetadata = metadata.getFiles().get(name);
    if (fileMetadata == null) {
      return null;
    }

    ensureFileDownloaded(name, fileMetadata);

    // slice from the container mmap
    final MappedByteBuffer container = containers[fileMetadata.getContainer()];
    final ByteBuffer view = container.asReadOnlyBuffer();
    view.position(Ints.checkedCast(fileMetadata.getStartOffset()))
        .limit(Ints.checkedCast(fileMetadata.getStartOffset() + fileMetadata.getSize()));
    return view.slice();
  }

  @Nullable
  @Override
  public ByteBuffer mapExternalFile(String filename, String name) throws IOException
  {
    checkClosed();
    final PartialSegmentFileMapperV10 externalMapper = externalMappers.get(filename);
    if (externalMapper == null) {
      throw DruidException.defensive("external file[%s] containing[%s] not found", filename, name);
    }
    return externalMapper.mapFile(name);
  }

  /**
   * Download every internal file referenced by this mapper's metadata (and recursively every attached external
   * mapper's metadata) so that {@link #isFullyDownloaded} returns true afterward. Used by the eager
   * {@code acquireSegment} path on partial-eligible segments to force the segment fully resident before returning.
   * Containers already fully downloaded are skipped.
   */
  public void ensureAllDownloaded() throws IOException
  {
    for (int containerIndex = 0; containerIndex < containers.length; containerIndex++) {
      downloadContainer(containerIndex);
    }
    for (PartialSegmentFileMapperV10 external : externalMappers.values()) {
      external.ensureAllDownloaded();
    }
  }

  /**
   * Download every container belonging to {@code bundleName} in this mapper, each in a single range read (see
   * {@link #downloadContainer}). No-op for an unknown bundle.
   */
  public void ensureBundleDownloaded(String bundleName) throws IOException
  {
    checkClosed();
    for (int containerIndex : getContainerIndicesForBundle(bundleName)) {
      downloadContainer(containerIndex);
    }
  }

  /**
   * Download an entire container in a single range read and mark every internal file it holds as downloaded. The whole
   * region streams straight to the container's local sparse file at offset 0.
   * <p>
   * No-op when every file in the container is already downloaded. A partially-downloaded container is re-fetched in
   * full (already-present files are overwritten with byte-identical data). Holds the container lock for the whole
   * fetch so a concurrent {@link #evictContainer} or container-init can't race the write; concurrent per-file
   * {@link #ensureFileDownloaded} calls write byte-identical data so they remain safe, and the download-bookkeeping is
   * gated on the atomic {@link #downloadedFiles} add so neither path double-counts.
   */
  private void downloadContainer(int containerIndex) throws IOException
  {
    final List<String> fileNames = containerFileNames.get(containerIndex);
    if (fileNames.isEmpty() || downloadedFiles.containsAll(fileNames)) {
      return;
    }
    containerLocks[containerIndex].lock();
    try {
      checkClosed();
      if (downloadedFiles.containsAll(fileNames)) {
        return;
      }
      ensureContainerInitialized(containerIndex);

      final SegmentFileContainerMetadata containerMeta = metadata.getContainers().get(containerIndex);
      streamRangeIntoContainer(
          containerIndex,
          headerSize + containerMeta.getStartOffset(),
          0,
          containerMeta.getSize(),
          StringUtils.format("container[%d]", containerIndex)
      );

      for (String name : fileNames) {
        markDownloaded(name, metadata.getFiles().get(name).getSize());
      }
    }
    finally {
      containerLocks[containerIndex].unlock();
    }
  }

  /**
   * Pre-download a set of internal files so that subsequent {@link #mapFile(String)} calls for these files will not
   * trigger individual downloads. Files that are already downloaded are skipped. Useful for batch-downloading all
   * files in a bundle at once (see {@link SegmentFileBuilder#startFileBundle}).
   */
  public void ensureFilesAvailable(Set<String> fileNames) throws IOException
  {
    for (String name : fileNames) {
      final SegmentInternalFileMetadata fileMetadata = metadata.getFiles().get(name);
      if (fileMetadata != null) {
        ensureFileDownloaded(name, fileMetadata);
      }
    }
  }

  /**
   * Total on-disk size of the header file(s) backing this mapper, summed across the main file and any external file
   * mappers. This is the actual reservation size that should be charged against the local cache once the metadata has
   * been fetched and persisted; callers can compare it against an up-front pessimistic estimate to decide whether to
   * shrink the reservation.
   */
  public long getOnDiskHeaderSize()
  {
    long total = headerFileSize(localCacheDir, targetFilename);
    for (PartialSegmentFileMapperV10 ext : externalMappers.values()) {
      total += headerFileSize(ext.localCacheDir, ext.targetFilename);
    }
    return total;
  }

  private static long headerFileSize(File dir, String filename)
  {
    final File header = new File(dir, filename + METADATA_HEADER_SUFFIX);
    return header.exists() ? header.length() : 0;
  }

  /**
   * Total bytes downloaded so far across all internal files, including external mappers.
   */
  public long getDownloadedBytes()
  {
    long total = downloadedBytes.get();
    for (PartialSegmentFileMapperV10 ext : externalMappers.values()) {
      total += ext.getDownloadedBytes();
    }
    return total;
  }

  /**
   * The internal file names that have been downloaded so far, scoped to this mapper. External mappers' downloaded
   * files are not included; call {@link #getDownloadedFiles()} on each external mapper directly if needed. Primarily
   * intended for tests and diagnostics.
   */
  public Set<String> getDownloadedFiles()
  {
    return Set.copyOf(downloadedFiles);
  }

  /**
   * Whether every internal file in this mapper's metadata (and recursively every attached external mapper's metadata)
   * is downloaded. Used by the sync cursor-factory path to verify that the segment has been fully loaded before
   * cursor construction; the async path is the only way to drive on-demand downloads.
   */
  public boolean isFullyDownloaded()
  {
    if (!downloadedFiles.containsAll(metadata.getFiles().keySet())) {
      return false;
    }
    for (PartialSegmentFileMapperV10 external : externalMappers.values()) {
      if (!external.isFullyDownloaded()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void close()
  {
    if (closed.compareAndSet(false, true)) {
      final Closer closer = Closer.create();
      closer.register(() -> ByteBufferUtils.unmap(bitmapBuffer));
      for (MappedByteBuffer buffer : containers) {
        if (buffer != null) {
          closer.register(() -> ByteBufferUtils.unmap(buffer));
        }
      }
      closer.registerAll(externalMappers.values());
      CloseableUtils.closeAndWrapExceptions(closer);
    }
  }

  private void checkClosed()
  {
    if (closed.get()) {
      throw DruidException.defensive("PartialSegmentFileMapperV10 is closed");
    }
  }

  /**
   * Compute the absolute byte offset of an internal file within the segment file in deep storage.
   */
  private long computeAbsoluteOffset(SegmentInternalFileMetadata fileMetadata)
  {
    final SegmentFileContainerMetadata container = metadata.getContainers().get(fileMetadata.getContainer());
    return headerSize + container.getStartOffset() + fileMetadata.getStartOffset();
  }

  private void ensureFileDownloaded(String name, SegmentInternalFileMetadata fileMetadata) throws IOException
  {
    // already downloaded, nothing to do
    if (downloadedFiles.contains(name)) {
      return;
    }

    final ReentrantLock lock = fileLocks.computeIfAbsent(name, k -> new ReentrantLock());
    lock.lock();
    try {
      checkClosed();

      if (downloadedFiles.contains(name)) {
        return;
      }

      ensureContainerInitialized(fileMetadata.getContainer());
      streamRangeIntoContainer(
          fileMetadata.getContainer(),
          computeAbsoluteOffset(fileMetadata),
          fileMetadata.getStartOffset(),
          fileMetadata.getSize(),
          StringUtils.format("file[%s]", name)
      );
      markDownloaded(name, fileMetadata.getSize());
    }
    finally {
      lock.unlock();
      fileLocks.remove(name, lock);
    }
  }

  /**
   * Public entry point for cache-layer code that wants to ensure a container is materialized before any data is
   * downloaded into it (e.g. when a per-bundle cache entry is mounted, the entry pre-allocates its container files
   * so that subsequent {@link #mapFile} calls have somewhere to write into and the cache layer can charge the
   * reservation up front).
   */
  public void initializeContainer(int containerIndex) throws IOException
  {
    checkClosed();
    ensureContainerInitialized(containerIndex);
  }

  /**
   * Reverse of {@link #initializeContainer(int)}: unmap the in-memory view of the container, delete the local
   * container file, and clear the bitmap bits + {@link #downloadedFiles} entries for every internal file that lived
   * in this container.
   * <p>
   * Used by per-bundle cache entries on unmount/eviction to release the disk and memory footprint of one bundle
   * without affecting other bundles sharing the same {@link PartialSegmentFileMapperV10}. After eviction, subsequent
   * {@link #mapFile} calls for files in this container will re-trigger downloads via {@link #initializeContainer}
   * and the bitmap will be repopulated incrementally.
   * <p>
   * <b>Concurrency contract.</b> The caller is responsible for ensuring no concurrent {@link #mapFile} (or
   * {@link #ensureFilesAvailable}) call is in flight for any file in this container. This is enforced one layer up
   * by the cache-entry refcount: {@code PartialSegmentBundleCacheEntry} only invokes {@code evictContainer} from its
   * {@code doActualUnmount} callback, which fires only after every reference acquired via {@code acquireReference()}
   * has been closed. Bypassing that gate is dangerous, {@link ByteBufferUtils#unmap} frees the off-heap mapping, so a
   * {@link ByteBuffer#slice} from a concurrent reader is a JVM SIGSEGV, not a recoverable error.
   * <p>
   * No-op if the container has not been initialized.
   */
  public void evictContainer(int containerIndex)
  {
    checkClosed();
    containerLocks[containerIndex].lock();
    try {
      final MappedByteBuffer existing = containers[containerIndex];
      if (existing != null) {
        ByteBufferUtils.unmap(existing);
        containers[containerIndex] = null;
      }
      // Try the cached containerFiles[i] first. If it's null, the container was never initialized in this mapper
      // instance (typical right after create() with an empty bitmap), but the on-disk file may still exist from a
      // previous run. Fall back to the deterministic path so eviction is always effective.
      File containerFile = containerFiles[containerIndex];
      if (containerFile == null) {
        containerFile = new File(
            localCacheDir,
            StringUtils.format("%s.container.%05d", targetFilename, containerIndex)
        );
      }
      if (containerFile.exists() && !containerFile.delete()) {
        LOG.warn(
            "Failed to delete container file[%s] during eviction of container[%d] for [%s]; leaking on disk",
            containerFile,
            containerIndex,
            targetFilename
        );
      }
      containerFiles[containerIndex] = null;
    }
    finally {
      containerLocks[containerIndex].unlock();
    }

    // clear bitmap bits + downloadedFiles entries for files that lived in this container. Iterates
    // metadata.getFiles() without external synchronization: SegmentFileMetadata is constructed once at mapper
    // creation and its file map is effectively immutable for the mapper's lifetime, so concurrent iteration is safe.
    for (Map.Entry<String, SegmentInternalFileMetadata> entry : metadata.getFiles().entrySet()) {
      if (entry.getValue().getContainer() != containerIndex) {
        continue;
      }
      final String fileName = entry.getKey();
      if (downloadedFiles.remove(fileName)) {
        downloadedBytes.addAndGet(-entry.getValue().getSize());
      }
      clearBitmapBit(fileName);
    }
  }

  private void clearBitmapBit(String name)
  {
    final Integer index = fileNameToIndex.get(name);
    if (index == null) {
      return;
    }
    final int byteIndex = index / 8;
    final int bitMask = 1 << (index % 8);
    bitmapLock.lock();
    try {
      final byte existing = bitmapBuffer.get(byteIndex);
      bitmapBuffer.put(byteIndex, (byte) (existing & ~bitMask));
    }
    finally {
      bitmapLock.unlock();
    }
  }

  /**
   * Initialize a local container file if not already done. Creates a sparse file at the original container size
   * and memory-maps it. The channel is closed immediately after mapping, the mmap persists independently, backed by
   * the kernel page cache. This avoids the risk of channel closure from thread interruption.
   */
  private void ensureContainerInitialized(int containerIndex) throws IOException
  {
    if (containers[containerIndex] != null) {
      return;
    }

    containerLocks[containerIndex].lock();
    try {
      if (containers[containerIndex] != null) {
        return;
      }

      final SegmentFileContainerMetadata containerMeta = metadata.getContainers().get(containerIndex);
      final File localFile = new File(
          localCacheDir,
          StringUtils.format("%s.container.%05d", targetFilename, containerIndex)
      );

      // create sparse file at original container size, mmap it, then close the channel immediately.
      // set containerFiles before containers so that when another thread sees containers[i] != null
      // (the fast-path check), containerFiles[i] is guaranteed to be set already.
      try (RandomAccessFile raf = new RandomAccessFile(localFile, "rw"); FileChannel channel = raf.getChannel()) {
        raf.setLength(containerMeta.getSize());
        containerFiles[containerIndex] = localFile;
        containers[containerIndex] = channel.map(
            FileChannel.MapMode.READ_ONLY,
            0,
            containerMeta.getSize()
        );
      }
    }
    finally {
      containerLocks[containerIndex].unlock();
    }
  }

  /**
   * Stream {@code size} bytes starting at {@code absoluteOffset} in the segment file (deep storage) into container
   * {@code containerIndex}'s local file at {@code localOffset}, via a short-lived {@link RandomAccessFile} (traditional
   * I/O rather than an NIO channel, so a thread interrupt can't close the channel mid-write; the container's read-only
   * mmap sees the bytes through the shared page cache). {@code what} names the unit being fetched (e.g.
   * {@code "file[foo]"} or {@code "container[2]"}) for the end-of-stream error message.
   */
  private void streamRangeIntoContainer(
      int containerIndex,
      long absoluteOffset,
      long localOffset,
      long size,
      String what
  ) throws IOException
  {
    // stream straight from deep storage to the local container file to avoid heap-buffering the whole range
    final long startNanos = System.nanoTime();
    try (ResourceHolder<byte[]> bufHolder = CompressedPools.getOutputBytes();
         InputStream is = rangeReader.readRange(targetFilename, absoluteOffset, size);
         RandomAccessFile raf = new RandomAccessFile(containerFiles[containerIndex], "rw")) {
      final byte[] buf = bufHolder.get();
      raf.seek(localOffset);
      long remaining = size;
      while (remaining > 0) {
        final int toRead = (int) Math.min(buf.length, remaining);
        final int read = is.read(buf, 0, toRead);
        if (read < 0) {
          throw DruidException.defensive(
              "unexpected end of stream for %s of [%s], expected[%s] more bytes",
              what,
              targetFilename,
              remaining
          );
        }
        raf.write(buf, 0, read);
        remaining -= read;
      }
    }
    // Report the completed deep-storage range read (reached only on success). One read may cover many files; this is
    // the actual request granularity, so it measures wire bytes + latency rather than bytes that became resident.
    downloadListener.onRangeRead(size, System.nanoTime() - startNanos);
  }

  /**
   * Record an internal file as downloaded once its bytes are on disk: gate on the atomic {@link #downloadedFiles} add
   * so a concurrent download of the same file via the other path (whole-container {@link #downloadContainer} vs
   * per-file {@link #ensureFileDownloaded}) doesn't double-count its size (both write byte-identical data), then add
   * its size and set its bitmap bit.
   */
  private void markDownloaded(String name, long size)
  {
    if (downloadedFiles.add(name)) {
      downloadedBytes.addAndGet(size);
      downloadListener.onBytesDownloaded(size);
      markDownloadedInBitmap(name);
    }
  }

  /**
   * Set the bit for a downloaded file in the bitmap. Single-byte read-modify-write on the mmap under
   * {@link #bitmapLock}. The OS flushes the mmap to disk.
   */
  private void markDownloadedInBitmap(String name)
  {
    final Integer index = fileNameToIndex.get(name);
    if (index == null) {
      return;
    }
    final int byteIndex = index / 8;
    final int bitMask = 1 << (index % 8);

    bitmapLock.lock();
    try {
      final byte existing = bitmapBuffer.get(byteIndex);
      bitmapBuffer.put(byteIndex, (byte) (existing | bitMask));
    }
    finally {
      bitmapLock.unlock();
    }
  }

  /**
   * Fetch the raw V10 header bytes from deep storage and write them to a local file. The bitmap region is not
   * included, it is created by {@link #mmapBitmap} after parsing. The file is parseable by
   * {@link SegmentFileMetadataReader#read(InputStream, ObjectMapper)}.
   */
  private static void fetchAndPersistHeader(
      SegmentRangeReader rangeReader,
      String targetFilename,
      File headerFile
  ) throws IOException
  {
    // read the fixed header to determine the metadata size, plus extra int possibly containing compressed length if
    // compression is enabled, else worst case only a few extra bytes
    final byte[] fixedHeader = new byte[SegmentFileMetadataReader.HEADER_SIZE + Integer.BYTES];
    try (InputStream headerStream = rangeReader.readRange(targetFilename, 0, fixedHeader.length)) {
      int offset = 0;
      while (offset < fixedHeader.length) {
        int read = headerStream.read(fixedHeader, offset, fixedHeader.length - offset);
        if (read < 0) {
          break;
        }
        offset += read;
      }
    }

    final ByteBuffer headerBuf = ByteBuffer.wrap(fixedHeader).order(ByteOrder.LITTLE_ENDIAN);
    final int metaLength = headerBuf.getInt(2);
    final CompressionStrategy compressionStrategy = CompressionStrategy.forId(headerBuf.get(1));

    // compute the remaining bytes, either metaLength, or if compressed, read the extra int we read
    final long remainingBytes;
    final int actualHeaderSize;
    if (CompressionStrategy.NONE == compressionStrategy) {
      remainingBytes = metaLength;
      actualHeaderSize = SegmentFileMetadataReader.HEADER_SIZE;
    } else {
      remainingBytes = headerBuf.getInt(SegmentFileMetadataReader.HEADER_SIZE);
      actualHeaderSize = fixedHeader.length;
    }

    // write fixed header + remaining metadata bytes to a local file atomically (write to temp, then rename)
    // to avoid leaving a partial file on disk if the process crashes mid-write
    FileUtils.mkdirp(headerFile.getParentFile());
    FileUtils.writeAtomically(headerFile, out -> {
      out.write(fixedHeader, 0, actualHeaderSize);
      try (InputStream remainingStream = rangeReader.readRange(
               targetFilename,
               actualHeaderSize,
               remainingBytes
           )) {
        ByteStreams.limit(remainingStream, remainingBytes).transferTo(out);
      }
      return null;
    });
  }

  /**
   * Parse metadata from the header file. Throws on any parse failure (corrupt JSON, truncated header, etc.).
   */
  public static SegmentFileMetadataReader.Result parseHeaderFile(
      File headerFile,
      ObjectMapper jsonMapper
  ) throws IOException
  {
    try (FileInputStream fis = new FileInputStream(headerFile)) {
      return SegmentFileMetadataReader.read(fis, jsonMapper);
    }
  }

  /**
   * Mmap the bitmap region of the header file as read-write. Extends the file if the bitmap region doesn't exist yet.
   * The channel is closed immediately after mapping.
   */
  private static MappedByteBuffer mmapBitmap(
      File headerFile,
      SegmentFileMetadataReader.Result result
  ) throws IOException
  {
    final int numBitmapBytes = (result.getMetadata().getFiles().size() + 7) / 8;
    final long expectedSize = result.getHeaderSize() + numBitmapBytes;
    try (RandomAccessFile raf = new RandomAccessFile(headerFile, "rw");
         FileChannel channel = raf.getChannel()) {
      if (raf.length() < expectedSize) {
        raf.setLength(expectedSize);
      }
      return channel.map(FileChannel.MapMode.READ_WRITE, result.getHeaderSize(), numBitmapBytes);
    }
  }
}
