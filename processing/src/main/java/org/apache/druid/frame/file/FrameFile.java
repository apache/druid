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

package org.apache.druid.frame.file;

import org.apache.datasketches.memory.Memory;
import org.apache.druid.frame.Frame;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.MappedByteBufferHandler;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.ReferenceCountingCloseableObject;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.EnumSet;

/**
 * A file containing {@link Frame} data.
 *
 * Frame files are written by {@link FrameFileWriter}.
 *
 * Frame files can optionally be partitioned, by providing partition numbers to the {@link FrameFileWriter#writeFrame}
 * method when creating the file. Partitions are contiguous within the frame file.
 *
 * Frame files can contain up to {@link Integer#MAX_VALUE} frames. Generally, frames are on the order of 1 MB in size,
 * so this allows well over a petabyte of data per file. Ought to be enough for anyone.
 *
 * Format:
 *
 * - 2 bytes: {@link FrameFileWriter#MAGIC}
 * - NNN bytes: sequence of {@link FrameFileWriter#MARKER_FRAME} followed by one compressed frame (see {@link Frame})
 * - 1 byte: {@link FrameFileWriter#MARKER_NO_MORE_FRAMES}
 * - 4 bytes * numPartitions: end frame number of each partition (exclusive), as little-endian ints. Note that
 * partitions may be empty. In this case, certain adjacent values in this array will be equal. Only present if the
 * file is partitioned.
 * - 8 bytes * numFrames: end of each compressed frame (exclusive), relative to start of file, as little-endian longs
 * - 4 bytes: number of frames, as little-endian int
 * - 4 bytes: number of partitions, as little-endian int
 * - 4 bytes: length of footer, from {@link FrameFileWriter#MARKER_NO_MORE_FRAMES} to EOF
 * - 4 bytes: checksum of footer (xxhash64, truncated to 32 bits), not considering these final 4 bytes
 *
 * Instances of this class are not thread-safe. For sharing across threads, use {@link #newReference()} to create
 * an additional reference.
 */
public class FrameFile implements Closeable
{
  private static final Logger log = new Logger(FrameFile.class);

  public enum Flag
  {
    /**
     * Delete the opened frame file when all references are closed.
     */
    DELETE_ON_CLOSE
  }

  private final File file;
  private final long fileLength;
  private final Memory footerMemory; // Footer is everything from the final MARKER_NO_MORE_FRAMES to EOF.
  private final int maxMmapSize;
  private final int numFrames;
  private final int numPartitions;
  private final ReferenceCountingCloseableObject<Closeable> referenceCounter;
  private final Closeable referenceReleaser;

  /**
   * Mapped memory, starting from {@link #bufferOffset} in {@link #file}, up to max of {@link #maxMmapSize}. Acts as
   * a window on the underlying file. Remapped using {@link #remapBuffer(long)}, freed using {@link #releaseBuffer()}.
   *
   * Even though managing multiple buffers requires extra code, we use this instead of {@link Memory#map(File)} for
   * two reasons:
   *
   * - Current version of {@link Memory#map(File)} is not compatible with Java 17.
   * - Using ByteBuffer-backed Memory enables zero-copy decompression in {@link Frame#decompress}.
   */
  private Memory buffer;

  /**
   * Offset of {@link #buffer} from the start of the file.
   */
  private long bufferOffset;

  /**
   * Runnable that unmaps {@link #buffer}.
   */
  private Runnable bufferCloser;

  private FrameFile(
      final File file,
      final long fileLength,
      final Memory footerMemory,
      @Nullable final Memory wholeFileMemory,
      final int maxMmapSize,
      final int numFrames,
      final int numPartitions,
      final ReferenceCountingCloseableObject<Closeable> referenceCounter,
      final Closeable referenceReleaser
  )
  {
    this.file = file;
    this.fileLength = fileLength;
    this.footerMemory = footerMemory;
    this.maxMmapSize = maxMmapSize;
    this.numFrames = numFrames;
    this.numPartitions = numPartitions;
    this.referenceCounter = referenceCounter;
    this.referenceReleaser = referenceReleaser;

    if (wholeFileMemory != null) {
      assert wholeFileMemory.getCapacity() == fileLength;

      // Set buffer, but not bufferCloser; if buffer was passed in constructor, it is shared across references,
      // and therefore is closed using referenceReleaser.
      buffer = wholeFileMemory;
    }
  }

  /**
   * Open a frame file with certain optional flags.
   *
   * @param file  ƒrame file
   * @param flags optional flags
   */
  public static FrameFile open(final File file, final Flag... flags) throws IOException
  {
    return open(file, Integer.MAX_VALUE, flags);
  }

  /**
   * Open a frame file with certain optional flags.
   *
   * Package-private because this method is intended for use in tests. In production, {@code maxMmapSize} is
   * set to {@link Integer#MAX_VALUE}.
   *
   * @param file        ƒrame file
   * @param maxMmapSize largest buffer to mmap at once
   * @param flags       optional flags
   */
  static FrameFile open(final File file, final int maxMmapSize, final Flag... flags) throws IOException
  {
    final EnumSet<Flag> flagSet = flags.length == 0 ? EnumSet.noneOf(Flag.class) : EnumSet.copyOf(Arrays.asList(flags));

    if (!file.exists()) {
      throw new FileNotFoundException(StringUtils.format("File [%s] not found", file));
    }

    // Closer for mmap that is shared across all references: either footer only (if file size is larger
    // Integer.MAX_VALUE) or entire file (if file size is smaller than, or equal to, Integer.MAX_VALUE).
    Closeable sharedMapCloser = null;

    try (final RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
      final long fileLength = randomAccessFile.length();

      // Verify minimum file length.
      if (fileLength <
          FrameFileWriter.MAGIC.length + FrameFileWriter.TRAILER_LENGTH + Byte.BYTES /* MARKER_NO_MORE_FRAMES */) {
        throw new IOE("File [%s] is too short (size = [%,d])", file, fileLength);
      }

      // Verify magic.
      final byte[] buf = new byte[FrameFileWriter.TRAILER_LENGTH /* Larger than FrameFileWriter.MAGIC */];
      final Memory bufMemory = Memory.wrap(buf, ByteOrder.LITTLE_ENDIAN);
      randomAccessFile.readFully(buf, 0, FrameFileWriter.MAGIC.length);

      if (!bufMemory.equalTo(0, Memory.wrap(FrameFileWriter.MAGIC), 0, FrameFileWriter.MAGIC.length)) {
        throw new IOE("File [%s] is not a frame file", file);
      }

      // Read number of frames and partitions.
      randomAccessFile.seek(fileLength - FrameFileWriter.TRAILER_LENGTH);
      randomAccessFile.readFully(buf, 0, FrameFileWriter.TRAILER_LENGTH);

      final int numFrames = bufMemory.getInt(0);
      final int numPartitions = bufMemory.getInt(Integer.BYTES);
      final int footerLength = bufMemory.getInt(Integer.BYTES * 2L);
      final int expectedFooterChecksum = bufMemory.getInt(Integer.BYTES * 3L);

      if (footerLength < 0) {
        throw new ISE("Negative-size footer. Corrupt or truncated file?");
      } else if (footerLength > fileLength) {
        throw new ISE("Oversize footer. Corrupt or truncated file?");
      }

      final Memory wholeFileMemory;
      final Memory footerMemory;

      if (fileLength <= maxMmapSize) {
        // Map entire file, use region for footer.
        final MappedByteBufferHandler mapHandle = FileUtils.map(randomAccessFile, 0, fileLength);
        sharedMapCloser = mapHandle;
        wholeFileMemory = Memory.wrap(mapHandle.get(), ByteOrder.LITTLE_ENDIAN);

        if (wholeFileMemory.getCapacity() != fileLength) {
          // Check that the mapped file is the expected length. May differ if the file was updated while we're trying
          // to map it.
          throw new ISE("Memory map size does not match file size");
        }

        footerMemory = wholeFileMemory.region(fileLength - footerLength, footerLength, ByteOrder.LITTLE_ENDIAN);
      } else {
        // Map footer only. Will map the entire file in pages later, using "remap".
        final MappedByteBufferHandler footerMapHandle =
            FileUtils.map(randomAccessFile, fileLength - footerLength, footerLength);
        sharedMapCloser = footerMapHandle;
        wholeFileMemory = null;
        footerMemory = Memory.wrap(footerMapHandle.get(), ByteOrder.LITTLE_ENDIAN);
      }

      // Verify footer begins with MARKER_NO_MORE_FRAMES.
      if (footerMemory.getByte(0) != FrameFileWriter.MARKER_NO_MORE_FRAMES) {
        throw new IOE("File [%s] end marker not in expected location", file);
      }

      // Verify footer checksum.
      final int actualChecksum =
          (int) footerMemory.xxHash64(0, footerMemory.getCapacity() - Integer.BYTES, FrameFileWriter.CHECKSUM_SEED);

      if (expectedFooterChecksum != actualChecksum) {
        throw new ISE("Expected footer checksum did not match actual checksum. Corrupt or truncated file?");
      }

      // Verify footer length.
      if (footerLength != FrameFileWriter.footerLength(numFrames, numPartitions)) {
        throw new ISE("Expected footer length did not match actual footer length. Corrupt or truncated file?");
      }

      // Set up closer, refcounter; return instance.
      final Closer fileCloser = Closer.create();
      fileCloser.register(sharedMapCloser);

      if (flagSet.contains(Flag.DELETE_ON_CLOSE)) {
        fileCloser.register(() -> {
          if (!file.delete()) {
            log.warn("Could not delete frame file [%s]", file);
          }
        });
      }

      final ReferenceCountingCloseableObject<Closeable> referenceCounter =
          new ReferenceCountingCloseableObject<Closeable>(fileCloser) {};

      return new FrameFile(
          file,
          fileLength,
          footerMemory,
          wholeFileMemory,
          maxMmapSize,
          numFrames,
          numPartitions,
          referenceCounter,
          referenceCounter
      );
    }
    catch (Throwable e) {
      // Close mapCloser, not fileCloser: if there is an error in "open" then we don't delete the file.
      if (e instanceof IOException) {
        // Don't wrap IOExceptions.
        throw CloseableUtils.closeInCatch((IOException) e, sharedMapCloser);
      } else {
        throw CloseableUtils.closeAndWrapInCatch(e, sharedMapCloser);
      }
    }
  }

  /**
   * Number of frames in the file.
   */
  public int numFrames()
  {
    return numFrames;
  }

  /**
   * Number of partitions in the file, or zero if the file is unpartitioned.
   */
  public int numPartitions()
  {
    return numPartitions;
  }

  /**
   * First frame of a given partition. Partitions beyond {@link #numPartitions()} are treated as empty: if provided,
   * this method returns {@link #numFrames()}.
   */
  public int getPartitionStartFrame(final int partition)
  {
    checkOpen();

    if (partition < 0) {
      throw new IAE("Partition [%,d] out of bounds", partition);
    } else if (partition >= numPartitions) {
      // Frame might not have every partition, if some are empty.
      return numFrames;
    } else {
      final long partitionStartFrameLocation =
          footerMemory.getCapacity()
          - FrameFileWriter.TRAILER_LENGTH
          - (long) numFrames * Long.BYTES
          - (long) (numPartitions - partition) * Integer.BYTES;

      return footerMemory.getInt(partitionStartFrameLocation);
    }
  }

  /**
   * Reads a frame from the file.
   */
  public Frame frame(final int frameNumber)
  {
    checkOpen();

    if (frameNumber < 0 || frameNumber >= numFrames) {
      throw new IAE("Frame [%,d] out of bounds", frameNumber);
    }

    final long frameEnd = getFrameEndPosition(frameNumber);
    final long frameStart;

    if (frameNumber == 0) {
      frameStart = FrameFileWriter.MAGIC.length + Byte.BYTES /* MARKER_FRAME */;
    } else {
      frameStart = getFrameEndPosition(frameNumber - 1) + Byte.BYTES /* MARKER_FRAME */;
    }

    if (buffer == null || frameStart < bufferOffset || frameEnd > bufferOffset + buffer.getCapacity()) {
      remapBuffer(frameStart);
    }

    if (frameStart < bufferOffset || frameEnd > bufferOffset + buffer.getCapacity()) {
      // Still out of bounds after remapping successfully: must mean frame was too large to fit in maxMmapSize.
      throw new ISE("Frame [%,d] too large (max size = %,d bytes)", frameNumber, maxMmapSize);
    }

    // Decompression is safe even on corrupt data: it validates position, length, checksum.
    return Frame.decompress(buffer, frameStart - bufferOffset, frameEnd - frameStart);
  }

  /**
   * Creates a new reference to this file. Calling {@link #close()} releases the reference. The original file
   * is closed when it, and all additional references, are closed.
   *
   * The new FrameFile instance may be used concurrently with the original FrameFile instance.
   */
  public FrameFile newReference()
  {
    final Closeable releaser = referenceCounter.incrementReferenceAndDecrementOnceCloseable()
                                               .orElseThrow(() -> new ISE("Frame file is closed"));

    return new FrameFile(
        file,
        fileLength,
        footerMemory,
        bufferOffset == 0 && bufferCloser == null ? buffer : null, // If bufferCloser is null, buffer is shared
        maxMmapSize,
        numFrames,
        numPartitions,
        referenceCounter,
        releaser
    );
  }

  /**
   * Returns the file that this instance is backed by.
   */
  public File file()
  {
    return file;
  }

  @Override
  public void close() throws IOException
  {
    CloseableUtils.closeAll(this::releaseBuffer, referenceReleaser);
  }

  /**
   * Checks if the frame file is open. If so, does nothing. If not, throws an exception.
   *
   * Racey, since this object can be used by multiple threads, but this is only meant as a last-ditch sanity check, not
   * a bulletproof precondition check.
   */
  private void checkOpen()
  {
    if (referenceCounter.isClosed()) {
      throw new ISE("Frame file is closed");
    }
  }

  private long getFrameEndPosition(final int frameNumber)
  {
    assert frameNumber >= 0 && frameNumber < numFrames;

    final long frameEndPointerPosition =
        footerMemory.getCapacity() - FrameFileWriter.TRAILER_LENGTH - (long) (numFrames - frameNumber) * Long.BYTES;

    final long frameEndPosition = footerMemory.getLong(frameEndPointerPosition);

    // Bounds check: protect against possibly-corrupt data.
    if (frameEndPosition < 0 || frameEndPosition > fileLength - footerMemory.getCapacity()) {
      throw new ISE("Corrupt frame file: frame [%,d] location out of range", frameNumber);
    }

    return frameEndPosition;
  }

  /**
   * Updates {@link #buffer}, {@link #bufferOffset}, and {@link #bufferCloser} to a new offset. Closes the old
   * buffer, if any.
   */
  private void remapBuffer(final long offset)
  {
    releaseBuffer();

    if (offset >= fileLength) {
      throw new IAE("Offset [%,d] out of range for file length [%,d]", offset, fileLength);
    }

    final MappedByteBufferHandler mapHandle;

    try {
      mapHandle = FileUtils.map(file, offset, Math.min(fileLength - offset, maxMmapSize));
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    buffer = Memory.wrap(mapHandle.get(), ByteOrder.LITTLE_ENDIAN);
    bufferCloser = mapHandle::close;
    bufferOffset = offset;
  }

  /**
   * Nulls out {@link #buffer} and {@link #bufferCloser} references.
   *
   * Explicitly frees {@link #buffer} if {@link #bufferCloser} is set. If {@link #buffer} is set, but
   * {@link #bufferCloser} is not set, it is a shared buffer, and is not freed.
   */
  private void releaseBuffer()
  {
    try {
      if (bufferCloser != null) {
        bufferCloser.run();
      }
    }
    finally {
      buffer = null;
      bufferCloser = null;
    }
  }
}
