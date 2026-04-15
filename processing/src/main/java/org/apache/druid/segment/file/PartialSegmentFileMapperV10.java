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
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.ByteBufferUtils;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
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
  static final String METADATA_HEADER_SUFFIX = ".header";

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
      List<String> externals
  ) throws IOException
  {
    final PartialSegmentFileMapperV10 entryPoint = createForFile(
        rangeReader,
        jsonMapper,
        localCacheDir,
        targetFilename
    );

    final Map<String, PartialSegmentFileMapperV10> externalMappers = new HashMap<>();
    try {
      for (String filename : externals) {
        externalMappers.put(
            filename,
            createForFile(rangeReader, jsonMapper, localCacheDir, filename)
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
      String targetFilename
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
        // corrupted file (partial write, truncated bitmap, bad JSON, etc.) — delete and re-fetch
        result = null;
        headerFile.delete();
      }
    }

    if (result == null) {
      fetchAndPersistHeader(rangeReader, targetFilename, headerFile);
      result = parseHeaderFile(headerFile, jsonMapper);
      bitmapBuffer = mmapBitmap(headerFile, result);
    }

    final PartialSegmentFileMapperV10 mapper = new PartialSegmentFileMapperV10(
        result.getMetadata(),
        result.getHeaderSize(),
        rangeReader,
        targetFilename,
        localCacheDir,
        bitmapBuffer
    );

    // restore downloaded files from the bitmap
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

  // external file mappers
  private final Map<String, PartialSegmentFileMapperV10> externalMappers = new HashMap<>();

  // track which internal files have been downloaded
  private final Set<String> downloadedFiles = ConcurrentHashMap.newKeySet();
  private final ConcurrentHashMap<String, ReentrantLock> fileLocks = new ConcurrentHashMap<>();
  private final ReentrantLock bitmapLock;
  private final MappedByteBuffer bitmapBuffer;
  private final AtomicLong downloadedBytes = new AtomicLong(0);
  private final AtomicBoolean closed = new AtomicBoolean(false);

  private PartialSegmentFileMapperV10(
      SegmentFileMetadata metadata,
      long headerSize,
      SegmentRangeReader rangeReader,
      String targetFilename,
      File localCacheDir,
      MappedByteBuffer bitmapBuffer
  )
  {
    this.metadata = metadata;
    this.headerSize = headerSize;
    this.rangeReader = rangeReader;
    this.targetFilename = targetFilename;
    this.localCacheDir = localCacheDir;
    this.bitmapBuffer = bitmapBuffer;

    // build stable file name ordering for bitmap indexing
    this.sortedFileNames = new ArrayList<>(new TreeSet<>(metadata.getFiles().keySet()));
    this.fileNameToIndex = new HashMap<>();
    for (int i = 0; i < sortedFileNames.size(); i++) {
      fileNameToIndex.put(sortedFileNames.get(i), i);
    }

    final int numContainers = metadata.getContainers().size();
    this.containers = new MappedByteBuffer[numContainers];
    this.containerFiles = new File[numContainers];
    this.containerLocks = new ReentrantLock[numContainers];
    for (int i = 0; i < numContainers; i++) {
      this.containerLocks[i] = new ReentrantLock();
    }
    this.bitmapLock = new ReentrantLock();
  }

  public SegmentFileMetadata getSegmentFileMetadata()
  {
    return metadata;
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
   * Pre-download a set of internal files so that subsequent {@link #mapFile(String)} calls for these files will not
   * trigger individual downloads. Files that are already downloaded are skipped. This is useful for batch-downloading
   * all files for a projection at once.
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
      downloadFileToContainer(name, fileMetadata);
      downloadedFiles.add(name);
      markDownloadedInBitmap(name);
    }
    finally {
      lock.unlock();
      fileLocks.remove(name, lock);
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
   * Download an internal file from deep storage and write it to the correct position in its local container file.
   * Uses a short-lived {@link RandomAccessFile} for writing. The mmap sees the written data through the shared page
   * cache.
   */
  private void downloadFileToContainer(String name, SegmentInternalFileMetadata fileMetadata) throws IOException
  {
    final long absoluteOffset = computeAbsoluteOffset(fileMetadata);
    final long size = fileMetadata.getSize();

    // stream directly from deep storage to the local container file to avoid holding the entire file in heap
    try (InputStream is = rangeReader.readRange(targetFilename, absoluteOffset, size);
         RandomAccessFile raf = new RandomAccessFile(containerFiles[fileMetadata.getContainer()], "rw")) {
      raf.seek(fileMetadata.getStartOffset());
      final byte[] buf = new byte[8192];
      long remaining = size;
      while (remaining > 0) {
        final int toRead = (int) Math.min(buf.length, remaining);
        final int read = is.read(buf, 0, toRead);
        if (read < 0) {
          throw DruidException.defensive(
              "unexpected end of stream for file[%s], expected[%s] more bytes",
              name,
              remaining
          );
        }
        raf.write(buf, 0, read);
        remaining -= read;
      }
    }

    downloadedBytes.addAndGet(size);
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
  private static SegmentFileMetadataReader.Result parseHeaderFile(
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
