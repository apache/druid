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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Sets;
import com.metamx.common.ISE;
import io.druid.timeline.DataSegment;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
*/
public class StorageLocation
{
  @JsonProperty
  private final File path;
  @JsonProperty
  private final long presetMaxSize;
  @JsonProperty
  private long maxSize;
  @JsonProperty
  private long currSize;
  private final Set<DataSegment> segments;
  private static final long BLOCK_SIZE_SHIFT = 12;                       // 1 << 12 - 4KB
  private static final long BLOCK_SIZE_MASK = (1 << BLOCK_SIZE_SHIFT) - 1; // 0x7FFF


  StorageLocation(File path, long maxSize)
  {
    this.path = path;
    this.currSize = getInitSize();
    this.presetMaxSize = maxSize;
    updateMaxSize();

    this.segments = Sets.newHashSet();
  }

  File getPath()
  {
    return path;
  }

  long getMaxSize()
  {
    updateMaxSize();

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

  boolean canHandle(long size)
  {
    return available() >= size;
  }

  synchronized long available()
  {
    return maxSize - currSize;
  }

  StorageLocation mostEmpty(StorageLocation other)
  {
    return available() > other.available() ? this : other;
  }

  private long getOccupySize(long size)
  {
    if ((size & BLOCK_SIZE_MASK) == 0) {
      return size;
    }
    return (((size >> BLOCK_SIZE_SHIFT) + 1) << BLOCK_SIZE_SHIFT);
  }

  private long getInitSize()
  {
    final AtomicLong size = new AtomicLong(0);

    try
    {
      Files.walkFileTree (
          path.toPath(),
          new SimpleFileVisitor<Path>()
          {
            @Override public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {

              size.addAndGet (getOccupySize(attrs.size()));
              return FileVisitResult.CONTINUE;
            }

            @Override public FileVisitResult
            visitFileFailed(Path file, IOException exc) {

              // Skip files that can't be visited
              return FileVisitResult.CONTINUE;
            }

            @Override public FileVisitResult
            postVisitDirectory (Path dir, IOException exc) {

              // Ignore errors traversing a folder
              return FileVisitResult.CONTINUE;
            }
          }
          );
    }
    catch (IOException e)
    {
      throw new ISE(e.getMessage());
    }

    return size.get();
  }

  private void updateMaxSize()
  {
    maxSize = getPossibleMaxSize(presetMaxSize);
  }

  private long getPossibleMaxSize(long maxSize)
  {
    long possibleMaxSize = path.getUsableSpace() + this.currSize;
    return (possibleMaxSize > maxSize) ? maxSize : possibleMaxSize;
  }
}
