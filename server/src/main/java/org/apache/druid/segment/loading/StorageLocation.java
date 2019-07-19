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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.FileUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.io.File;
import java.util.HashSet;
import java.util.Set;

/**
*/
public class StorageLocation
{
  private static final Logger log = new Logger(StorageLocation.class);

  private final File path;
  private final long maxSize;
  private final long freeSpaceToKeep;
  private final Set<File> files = new HashSet<>();

  private volatile long currSize = 0;

  public StorageLocation(File path, long maxSize, @Nullable Double freeSpacePercent)
  {
    this.path = path;
    this.maxSize = maxSize;

    if (freeSpacePercent != null) {
      long totalSpaceInPartition = path.getTotalSpace();
      this.freeSpaceToKeep = (long) ((freeSpacePercent * totalSpaceInPartition) / 100);
      log.info(
          "SegmentLocation[%s] will try and maintain [%d:%d] free space while loading segments.",
          path,
          freeSpaceToKeep,
          totalSpaceInPartition
      );
    } else {
      this.freeSpaceToKeep = 0;
    }
  }

  public File getPath()
  {
    return path;
  }

  public long getMaxSize()
  {
    return maxSize;
  }

  /**
   * Remove a segment file from this location. The given file argument must be a file rather than directory.
   */
  public synchronized void removeFile(File file)
  {
    if (files.remove(file)) {
      currSize -= FileUtils.sizeOf(file);
    }
  }

  /**
   * Remove a segment dir from this location. The segment size is subtracted from currSize.
   */
  public synchronized void removeSegmentDir(File segmentDir, DataSegment segment)
  {
    if (files.remove(segmentDir)) {
      currSize -= segment.getSize();
    }
  }

  /**
   * Reserves space to store the given segment. The segment size is added to currSize.
   * Returns true if it succeeds to add the given file.
   */
  public synchronized boolean reserve(File segmentDir, DataSegment segment)
  {
    return reserve(segmentDir, segment.getId().toString(), segment.getSize());
  }

  /**
   * Reserves space to store the given segment. Returns true if it succeeds to add the given file.
   */
  public synchronized boolean reserve(File segmentFileToAdd, String segmentId, long segmentSize)
  {
    if (files.contains(segmentFileToAdd)) {
      return false;
    }
    if (canHandle(segmentId, segmentSize)) {
      files.add(segmentFileToAdd);
      currSize += segmentSize;
      return true;
    } else {
      return false;
    }
  }

  /**
   * This method is available for only unit tests. Production code must use {@link #reserve} instead.
   */
  @VisibleForTesting
  boolean canHandle(String segmentId, long segmentSize)
  {
    if (available() < segmentSize) {
      log.warn(
          "Segment[%s:%,d] too large for storage[%s:%,d]. Check your druid.segmentCache.locations maxSize param",
          segmentId, segmentSize, getPath(), available()
      );
      return false;
    }

    if (freeSpaceToKeep > 0) {
      long currFreeSpace = path.getFreeSpace();
      if ((freeSpaceToKeep + segmentSize) > currFreeSpace) {
        log.warn(
            "Segment[%s:%,d] too large for storage[%s:%,d] to maintain suggested freeSpace[%d], current freeSpace is [%d].",
            segmentId,
            segmentSize,
            getPath(),
            available(),
            freeSpaceToKeep,
            currFreeSpace
        );
        return false;
      }
    }

    return true;
  }

  public synchronized long available()
  {
    return maxSize - currSize;
  }
}
