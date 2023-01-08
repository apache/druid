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
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.ISE;

import java.nio.ByteOrder;

/**
 * Encapsulation for ƒrame file footer related operations. The footer must be wrapped in a memory object (the memory
 * can be physical or mmaped). Some verifications are also done on the footer to see if it is not corrupted.
 * The schema for footer is as described by {@link FrameFile}.
 */
public class FrameFileFooter
{
  private final Memory footerMemory;
  private final int numFrames;
  private final int numPartitions;
  private final long frameFileLength;

  public FrameFileFooter(Memory footerMemory, long frameFileLength) throws IOE
  {
    this.footerMemory = footerMemory;
    this.frameFileLength = frameFileLength;

    Memory trailer = footerMemory.region(
        footerMemory.getCapacity() - FrameFileWriter.TRAILER_LENGTH,
        FrameFileWriter.TRAILER_LENGTH,
        ByteOrder.LITTLE_ENDIAN
    );
    this.numFrames = trailer.getInt(0);
    this.numPartitions = trailer.getInt(Integer.BYTES);
    int length = trailer.getInt(Integer.BYTES * 2L);
    int expectedFooterChecksum = trailer.getInt(Integer.BYTES * 3L);
    // Verify footer begins with MARKER_NO_MORE_FRAMES.
    if (footerMemory.getByte(0) != FrameFileWriter.MARKER_NO_MORE_FRAMES) {
      throw new IOE("File [%s] end marker not in expected location", "file");
    }

    // Verify footer checksum.
    final int actualChecksum =
        (int) footerMemory.xxHash64(0, footerMemory.getCapacity() - Integer.BYTES, FrameFileWriter.CHECKSUM_SEED);

    if (expectedFooterChecksum != actualChecksum) {
      throw new ISE("Expected footer checksum did not match actual checksum. Corrupt or truncated file?");
    }

    // Verify footer length.
    if (length != FrameFileWriter.footerLength(numFrames, numPartitions)) {
      throw new ISE("Expected footer length did not match actual footer length. Corrupt or truncated file?");
    }
  }

  /**
   * First frame of a given partition. Partitions beyond {@link #getNumPartitions()} are treated as empty: if provided,
   * this method returns {@link #getNumFrames()}.
   */
  public int getPartitionStartFrame(final int partition)
  {
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
   * Get the last byte for the frame specified. The byte number is offsetted from the frame file start and is exclusive.
   * @param frameNumber the id of the frame to get the end position for
   * @return a long exclusive index representing the frame end
   */
  public long getFrameEndPosition(final int frameNumber)
  {
    assert frameNumber >= 0 && frameNumber < numFrames;

    final long frameEndPointerPosition =
        footerMemory.getCapacity() - FrameFileWriter.TRAILER_LENGTH - (long) (numFrames - frameNumber) * Long.BYTES;

    final long frameEndPosition = footerMemory.getLong(frameEndPointerPosition);

    // Bounds check: protect against possibly-corrupt data.
    if (frameEndPosition < 0 || frameEndPosition > frameFileLength - footerMemory.getCapacity()) {
      throw new ISE("Corrupt frame file: frame [%,d] location out of range", frameNumber);
    }

    return frameEndPosition;
  }

  public int getNumFrames()
  {
    return numFrames;
  }

  public int getNumPartitions()
  {
    return numPartitions;
  }
}
