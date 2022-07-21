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

import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import org.apache.datasketches.memory.MapHandle;
import org.apache.datasketches.memory.Memory;
import org.apache.druid.frame.Frame;
import org.apache.druid.java.util.common.ByteBufferUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.ReferenceCountingCloseableObject;
import org.apache.druid.utils.CloseableUtils;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
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
 * - 4 bytes * numPartitions: end frame number of each partition (exclusive), as little-endian ints. Note that
 * partitions may be empty. In this case, certain adjacent values in this array will be equal. Only present if the
 * file is partitioned.
 * - 1 byte: {@link FrameFileWriter#MARKER_NO_MORE_FRAMES}
 * - 8 bytes * numFrames: end of each compressed frame (exclusive), relative to start of file, as little-endian longs
 * - 4 bytes: number of frames, as little-endian int
 * - 4 bytes: number of partitions, as little-endian int
 */
public class FrameFile implements Closeable
{
  private static final Logger log = new Logger(FrameFile.class);

  public enum Flag
  {
    /**
     * Delete the opened frame file when all references are closed.
     */
    DELETE_ON_CLOSE,

    /**
     * Map using ByteBuffer. Used only for testing.
     */
    BB_MEMORY_MAP,

    /**
     * Map using DataSketches Memory. Used only for testing.
     */
    DS_MEMORY_MAP
  }

  private final File file;
  private final Memory memory;
  private final int numFrames;
  private final int numPartitions;
  private final ReferenceCountingCloseableObject<Closeable> referenceCounter;
  private final Closeable referenceReleaser;

  private FrameFile(
      final File file,
      final Memory memory,
      final int numFrames,
      final int numPartitions,
      final ReferenceCountingCloseableObject<Closeable> referenceCounter,
      final Closeable referenceReleaser
  )
  {
    this.file = file;
    this.memory = memory;
    this.numFrames = numFrames;
    this.numPartitions = numPartitions;
    this.referenceCounter = referenceCounter;
    this.referenceReleaser = referenceReleaser;
  }

  /**
   * Open a frame file with certain optional flags.
   */
  public static FrameFile open(final File file, final Flag... flags) throws IOException
  {
    final EnumSet<Flag> flagSet = flags.length == 0 ? EnumSet.noneOf(Flag.class) : EnumSet.copyOf(Arrays.asList(flags));

    if (!file.exists()) {
      throw new FileNotFoundException(StringUtils.format("File [%s] not found", file));
    }

    final Pair<Memory, Closeable> map = mapFile(file, flagSet);
    final Memory memory = map.lhs;
    final Closeable mapCloser = Preconditions.checkNotNull(map.rhs, "closer");

    try {
      // Verify minimum file length.
      if (memory.getCapacity() < FrameFileWriter.MAGIC.length + FrameFileWriter.TRAILER_LENGTH) {
        throw new IOE("File [%s] is too short for magic + trailer", file);
      }

      // Verify magic.
      if (!memory.equalTo(0, Memory.wrap(FrameFileWriter.MAGIC), 0, FrameFileWriter.MAGIC.length)) {
        throw new IOE("File [%s] is not a frame file", file);
      }

      final int numFrames = memory.getInt(memory.getCapacity() - Integer.BYTES * 2L);
      final int numPartitions = memory.getInt(memory.getCapacity() - Integer.BYTES);

      // Verify last frame is followed by MARKER_NO_MORE_FRAMES.
      final long endMarkerPosition;

      if (numFrames > 0) {
        endMarkerPosition = getFrameEndPosition(memory, numFrames - 1, numFrames);
      } else {
        endMarkerPosition = FrameFileWriter.MAGIC.length;
      }

      if (endMarkerPosition >= memory.getCapacity()) {
        throw new IOE("File [%s] end marker location out of range", file);
      }

      if (memory.getByte(endMarkerPosition) != FrameFileWriter.MARKER_NO_MORE_FRAMES) {
        throw new IOE("File [%s] end marker not in expected location", file);
      }

      final Closer fileCloser = Closer.create();
      fileCloser.register(mapCloser);

      if (flagSet.contains(Flag.DELETE_ON_CLOSE)) {
        fileCloser.register(() -> {
          if (!file.delete()) {
            log.warn("Could not delete frame file [%s]", file);
          }
        });
      }

      final ReferenceCountingCloseableObject<Closeable> referenceCounter =
          new ReferenceCountingCloseableObject<Closeable>(fileCloser) {};

      return new FrameFile(file, memory, numFrames, numPartitions, referenceCounter, referenceCounter);
    }
    catch (Throwable e) {
      // Close mapCloser, not fileCloser: if there is an error in "open" then we don't delete the file.
      if (e instanceof IOException) {
        // Don't wrap IOExceptions.
        throw CloseableUtils.closeInCatch((IOException) e, mapCloser);
      } else {
        throw CloseableUtils.closeAndWrapInCatch(e, mapCloser);
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
          memory.getCapacity()
          - FrameFileWriter.TRAILER_LENGTH
          - (long) numFrames * Long.BYTES
          - (long) (numPartitions - partition) * Integer.BYTES;

      // Bounds check: protect against possibly-corrupt data.
      if (partitionStartFrameLocation < 0 || partitionStartFrameLocation > memory.getCapacity() - Integer.BYTES) {
        throw new ISE("Corrupt frame file: partition [%,d] marker out of range", partition);
      }

      return memory.getInt(partitionStartFrameLocation);
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

    final long frameEnd = getFrameEndPosition(memory, frameNumber, numFrames);
    final long frameStart;

    if (frameNumber == 0) {
      frameStart = FrameFileWriter.MAGIC.length + Byte.BYTES /* MARKER_FRAME */;
    } else {
      frameStart = getFrameEndPosition(memory, frameNumber - 1, numFrames) + Byte.BYTES /* MARKER_FRAME */;
    }

    // Decompression is safe even on corrupt data: it validates position, length, checksum.
    return Frame.decompress(memory, frameStart, frameEnd - frameStart);
  }

  /**
   * Creates a new reference to this file. Calling {@link #close()} releases the reference. The original file
   * is closed when it, and all additional references, are closed.
   */
  public FrameFile newReference()
  {
    final Closeable releaser = referenceCounter.incrementReferenceAndDecrementOnceCloseable()
                                               .orElseThrow(() -> new ISE("Frame file is closed"));

    return new FrameFile(file, memory, numFrames, numPartitions, referenceCounter, releaser);
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
    referenceReleaser.close();
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

  /**
   * Maps a file, respecting the flags provided to {@link #open}.
   */
  private static Pair<Memory, Closeable> mapFile(final File file, final EnumSet<Flag> flagSet) throws IOException
  {
    if (flagSet.contains(Flag.DS_MEMORY_MAP) && flagSet.contains(Flag.BB_MEMORY_MAP)) {
      throw new ISE("Cannot open with both [%s] and [%s]", Flag.DS_MEMORY_MAP, Flag.BB_MEMORY_MAP);
    } else if (flagSet.contains(Flag.DS_MEMORY_MAP)) {
      return mapFileDS(file);
    } else if (flagSet.contains(Flag.BB_MEMORY_MAP)) {
      return mapFileBB(file);
    } else if (file.length() <= Integer.MAX_VALUE) {
      // Prefer using ByteBuffer for small files, because "frame" can use it to avoid a copy when decompressing.
      return mapFileBB(file);
    } else {
      return mapFileDS(file);
    }
  }

  /**
   * Maps a file using a MappedByteBuffer. This is preferred for small files, since it enables zero-copy decompression
   * in {@link #open}.
   */
  private static Pair<Memory, Closeable> mapFileBB(final File file) throws IOException
  {
    final MappedByteBuffer byteBuffer = Files.map(file, FileChannel.MapMode.READ_ONLY);
    byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
    return Pair.of(Memory.wrap(byteBuffer, ByteOrder.LITTLE_ENDIAN), () -> ByteBufferUtils.unmap(byteBuffer));
  }

  /**
   * Maps a file using the functionality in datasketches-memory.
   */
  private static Pair<Memory, Closeable> mapFileDS(final File file)
  {
    final MapHandle mapHandle = Memory.map(file, 0, file.length(), ByteOrder.LITTLE_ENDIAN);
    return Pair.of(
        mapHandle.get(),
        () -> {
          try {
            mapHandle.close();
          }
          catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
    );
  }

  private static long getFrameEndPosition(final Memory memory, final int frameNumber, final int numFrames)
  {
    final long frameEndPointerPosition =
        memory.getCapacity() - FrameFileWriter.TRAILER_LENGTH - (long) (numFrames - frameNumber) * Long.BYTES;

    // Bounds check: protect against possibly-corrupt data.
    if (frameEndPointerPosition < 0 || frameEndPointerPosition > memory.getCapacity() - Long.BYTES) {
      throw new ISE("Corrupt frame file: frame [%,d] location out of range", frameNumber);
    }

    return memory.getLong(frameEndPointerPosition);
  }
}
