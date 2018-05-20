/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.loading;

import com.google.common.collect.Sets;
import io.druid.java.util.common.logger.Logger;
import io.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.io.File;
import java.util.Set;

/**
*/
class StorageLocation
{
  private static final Logger log = new Logger(StorageLocation.class);

  private final File path;
  private final long maxSize;
  private final long freeSpaceToKeep;
  private final Set<DataSegment> segments;

  private volatile long currSize = 0;

  StorageLocation(File path, long maxSize, @Nullable Double freeSpacePercent)
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

    this.segments = Sets.newHashSet();
  }

  File getPath()
  {
    return path;
  }

  long getMaxSize()
  {
    return maxSize;
  }

  synchronized void addSegment(DataSegment segment)
  {
    if (segments.add(segment)) {
      currSize += segment.getSize();
    }
  }

  synchronized void removeSegment(DataSegment segment)
  {
    if (segments.remove(segment)) {
      currSize -= segment.getSize();
    }
  }

  boolean canHandle(DataSegment segment)
  {
    if (available() < segment.getSize()) {
      log.warn(
          "Segment[%s:%,d] too lage for storage[%s:%,d].",
          segment.getIdentifier(), segment.getSize(), getPath(), available()
      );
      return false;
    }

    if (freeSpaceToKeep > 0) {
      long currFreeSpace = path.getFreeSpace();
      if ((freeSpaceToKeep + segment.getSize()) > currFreeSpace) {
        log.warn(
            "Segment[%s:%,d] too large for storage[%s:%,d] to maintain suggested freeSpace[%d], current freeSpace is [%d].",
            segment.getIdentifier(),
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

  synchronized long available()
  {
    return maxSize - currSize;
  }
}
