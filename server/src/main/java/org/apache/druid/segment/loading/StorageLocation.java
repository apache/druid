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

import org.apache.commons.io.FileUtils;
import org.apache.druid.java.util.common.ISE;
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
   * Add a new file to this location. The given file argument must be a file rather than directory.
   */
  public synchronized void addFile(File file)
  {
    if (file.isDirectory()) {
      throw new ISE("[%s] must be a file. Use a");
    }
    if (files.add(file)) {
      currSize += FileUtils.sizeOf(file);
    }
  }

  /**
   * Add a new segment dir to this location. The segment size is added to currSize.
   */
  public synchronized void addSegmentDir(File segmentDir, DataSegment segment)
  {
    if (files.add(segmentDir)) {
      currSize += segment.getSize();
    }
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

  public boolean canHandle(DataSegment segment)
  {
    if (available() < segment.getSize()) {
      log.warn(
          "Segment[%s:%,d] too large for storage[%s:%,d]. Check your druid.segmentCache.locations maxSize param",
          segment.getId(), segment.getSize(), getPath(), available()
      );
      return false;
    }

    if (freeSpaceToKeep > 0) {
      long currFreeSpace = path.getFreeSpace();
      if ((freeSpaceToKeep + segment.getSize()) > currFreeSpace) {
        log.warn(
            "Segment[%s:%,d] too large for storage[%s:%,d] to maintain suggested freeSpace[%d], current freeSpace is [%d].",
            segment.getId(),
            segment.getSize(),
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
