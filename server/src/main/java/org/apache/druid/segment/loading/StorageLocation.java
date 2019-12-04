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
import javax.annotation.concurrent.GuardedBy;
import java.io.File;
import java.util.HashSet;
import java.util.Set;

/**
 * This class is a very simple logical representation of a local path. It keeps track of files stored under the
 * {@link #path} via {@link #reserve}, so that the total size of stored files doesn't exceed the {@link #maxSizeBytes}
 * and available space is always kept smaller than {@link #freeSpaceToKeep}.
 *
 * This class is thread-safe, so that multiple threads can update its state at the same time.
 * One example usage is that a historical can use multiple threads to load different segments in parallel
 * from deep storage.
*/
public class StorageLocation
{
  private static final Logger log = new Logger(StorageLocation.class);

  private final File path;
  private final long maxSizeBytes;
  private final long freeSpaceToKeep;

  /**
   * Set of files stored under the {@link #path}.
   */
  @GuardedBy("this")
  private final Set<File> files = new HashSet<>();

  /**
   * Current total size of files in bytes.
   */
  @GuardedBy("this")
  private long currSizeBytes = 0;

  public StorageLocation(File path, long maxSizeBytes, @Nullable Double freeSpacePercent)
  {
    this.path = path;
    this.maxSizeBytes = maxSizeBytes;

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

  /**
   * Remove a segment file from this location. The given file argument must be a file rather than directory.
   */
  public synchronized void removeFile(File file)
  {
    if (files.remove(file)) {
      currSizeBytes -= FileUtils.sizeOf(file);
    } else {
      log.warn("File[%s] is not found under this location[%s]", file, path);
    }
  }

  /**
   * Remove a segment dir from this location. The segment size is subtracted from currSizeBytes.
   */
  public synchronized void removeSegmentDir(File segmentDir, DataSegment segment)
  {
    if (files.remove(segmentDir)) {
      currSizeBytes -= segment.getSize();
    } else {
      log.warn("SegmentDir[%s] is not found under this location[%s]", segmentDir, path);
    }
  }

  /**
   * Reserves space to store the given segment. The segment size is added to currSizeBytes.
   * If it succeeds, it returns a file for the given segmentDir in this storage location. Returns null otherwise.
   */
  @Nullable
  public synchronized File reserve(String segmentDir, DataSegment segment)
  {
    return reserve(segmentDir, segment.getId().toString(), segment.getSize());
  }

  /**
   * Reserves space to store the given segment.
   * If it succeeds, it returns a file for the given segmentFilePathToAdd in this storage location.
   * Returns null otherwise.
   */
  @Nullable
  public synchronized File reserve(String segmentFilePathToAdd, String segmentId, long segmentSize)
  {
    final File segmentFileToAdd = new File(path, segmentFilePathToAdd);
    if (files.contains(segmentFileToAdd)) {
      return null;
    }
    if (canHandle(segmentId, segmentSize)) {
      files.add(segmentFileToAdd);
      currSizeBytes += segmentSize;
      return segmentFileToAdd;
    } else {
      return null;
    }
  }

  /**
   * This method is only package-private to use it in unit tests. Production code must not call this method directly.
   * Use {@link #reserve} instead.
   */
  @VisibleForTesting
  @GuardedBy("this")
  boolean canHandle(String segmentId, long segmentSize)
  {
    if (availableSizeBytes() < segmentSize) {
      log.warn(
          "Segment[%s:%,d] too large for storage[%s:%,d]. Check your druid.segmentCache.locations maxSize param",
          segmentId, segmentSize, getPath(), availableSizeBytes()
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
            availableSizeBytes(),
            freeSpaceToKeep,
            currFreeSpace
        );
        return false;
      }
    }

    return true;
  }

  public synchronized long availableSizeBytes()
  {
    return maxSizeBytes - currSizeBytes;
  }

  public synchronized long currSizeBytes()
  {
    return currSizeBytes;
  }
}
